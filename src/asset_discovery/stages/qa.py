"""Stage 6: QA — pydantic-ai agent evaluates coverage and fills gaps."""

from __future__ import annotations

from typing import Any

from pydantic_ai import Agent

from ..config import Config, _to_pydantic_ai_model
from ..cost import CostTracker
from ..db import get_connection, save_discovered_assets, save_discovered_urls
from ..display import show_detail, show_stage
from ..models import Asset, ExtractedAsset, QAReport
from .discover import _build_search_tools
from .prompts import QA_SYSTEM


async def run_qa(
    issuer_id: str, context_doc: str, assets: list[Asset], config: Config,
    rag_store=None, costs: CostTracker | None = None,
) -> QAReport:
    """Run QA agent to evaluate asset coverage and fill gaps."""
    show_stage(6, "Quality assurance")

    asset_summary = _build_asset_summary(assets)
    system_prompt = f"{context_doc}\n\n---\n\n{QA_SYSTEM}"

    search_tools, builtin_tools = _build_search_tools(config)

    qa_tools = []

    if rag_store:
        async def rag_query(query: str, top_k: int = 20) -> list[dict]:
            """Search already-scraped pages for specific information.

            Use targeted queries like "distribution center addresses" or
            "headquarters location" — not generic "all assets".
            Returns relevant page chunks with content, URL, and relevance score.
            """
            from rag import Usage as RAGUsage
            rag_usage = RAGUsage()
            results = await rag_store.query(
                query, namespace=issuer_id, top_k=top_k, usage=rag_usage,
            )
            if costs:
                costs.track_embedding(rag_usage.embedding_tokens)
                costs.track_cohere_rerank(rag_usage.rerank_calls)
            # Group by source page for better context
            pages: dict[str, list[str]] = {}
            for r in results:
                url = r.get("metadata", {}).get("url", "unknown")
                pages.setdefault(url, []).append(r.get("content", ""))
            return [
                {"url": url, "chunks": len(chunks),
                 "content": "\n\n".join(chunks[:3])}  # top 3 chunks per page
                for url, chunks in pages.items()
            ]
        qa_tools.append(rag_query)

        async def rag_extract(
            query: str, top_k: int = 20,
        ) -> list[dict[str, Any]]:
            """Search scraped pages and extract assets from the results.

            Use for targeted gap-filling: "Sprouts distribution center addresses",
            "headquarters office location", "warehouse facilities".
            Retrieves relevant page content via RAG, then runs LLM extraction.
            """
            from doc_extractor import extract, Document
            from rag import Usage as RAGUsage

            rag_usage = RAGUsage()
            results = await rag_store.query(
                query, namespace=issuer_id, top_k=top_k, usage=rag_usage,
            )
            if costs:
                costs.track_embedding(rag_usage.embedding_tokens)
                costs.track_cohere_rerank(rag_usage.rerank_calls)

            if not results:
                return []

            # Group chunks by page, rebuild page-level context
            pages: dict[str, list[str]] = {}
            for r in results:
                url = r.get("metadata", {}).get("url", "unknown")
                pages.setdefault(url, []).append(r.get("content", ""))

            # Extract from the top pages' combined content
            docs = [
                Document(
                    content="\n\n".join(chunks),
                    metadata={"url": url},
                )
                for url, chunks in list(pages.items())[:5]  # top 5 pages
            ]

            prompt = (
                f"Extract physical assets for {assets[0].entity_name if assets else 'the company'} "
                f"from these page excerpts. Focus on: {query}"
            )
            try:
                extracted = await extract(
                    documents=docs, schema=ExtractedAsset, prompt=prompt,
                    model=config.extract_model, max_concurrency=5,
                )
                # Dedup against existing
                existing_names = {a.asset_name.lower().strip() for a in assets if a.asset_name}
                new_assets = [
                    Asset(**e.model_dump())
                    for e in extracted
                    if e.asset_name.lower().strip() not in existing_names
                ]
                if new_assets:
                    show_detail(f"  RAG extract: {len(new_assets)} new assets for '{query[:50]}'")
                    assets.extend(new_assets)
                return [a.model_dump() for a in new_assets]
            except Exception as e:
                show_detail(f"  RAG extract failed: {e}")
                return []

        qa_tools.append(rag_extract)

    async def scrape_and_extract(urls: list[str]) -> list[dict[str, Any]]:
        """Scrape new URLs, extract assets, persist everything.
        Use only when RAG doesn't have the information — this is expensive.
        """
        from .scrape import run_scrape
        from .extract import run_extract
        url_dicts = [{"url": u, "category": "external", "notes": "QA gap-fill"} for u in urls]
        conn = get_connection(config)
        try:
            save_discovered_urls(conn, issuer_id, url_dicts)
        finally:
            conn.close()
        pages = await run_scrape(issuer_id, url_dicts, config, rag_store, costs=costs)
        new_assets = await run_extract(issuer_id, "", pages, config, costs=costs)
        if new_assets:
            existing_names = {a.asset_name.lower().strip() for a in assets if a.asset_name}
            deduped = [a for a in new_assets if a.asset_name.lower().strip() not in existing_names]
            if deduped:
                conn = get_connection(config)
                try:
                    save_discovered_assets(conn, issuer_id, [a.model_dump() for a in deduped])
                finally:
                    conn.close()
                assets.extend(deduped)
            new_assets = deduped
        return [a.model_dump() for a in new_assets]

    qa_tools.append(scrape_and_extract)

    async def remove_assets(indices: list[int]) -> int:
        """Remove assets by index from the current asset list.

        Use to clean up junk that got through extraction — things like
        "annual meeting venue", aggregate descriptions, or obviously
        wrong entries. Returns count removed.
        """
        removed = 0
        for idx in sorted(indices, reverse=True):
            if 0 <= idx < len(assets):
                assets.pop(idx)
                removed += 1
        if removed:
            show_detail(f"  Removed {removed} junk assets")
        return removed

    qa_tools.append(remove_assets)

    async def standardize_types(replacements: dict[str, str]) -> int:
        """Standardize asset_type_raw values.

        Pass a dict of {old_value: new_value} to clean up messy types.
        e.g. {"principal executive offices / support office": "corporate office"}
        Returns count updated.
        """
        updated = 0
        for asset in assets:
            if asset.asset_type_raw in replacements:
                asset.asset_type_raw = replacements[asset.asset_type_raw]
                updated += 1
        if updated:
            show_detail(f"  Standardized {updated} asset types")
        return updated

    qa_tools.append(standardize_types)

    model_str = _to_pydantic_ai_model(config.qa_model)
    if builtin_tools and model_str.startswith("openai:"):
        model_str = model_str.replace("openai:", "openai-responses:", 1)

    agent = Agent(
        model_str,
        system_prompt=system_prompt,
        output_type=QAReport,
        tools=qa_tools + search_tools,
        builtin_tools=builtin_tools,
    )

    qa_report = QAReport()
    async with agent:
        for _iteration in range(config.max_qa_iterations):
            result = await agent.run(
                f"## Current assets ({len(assets)} total)\n{asset_summary}\n\n"
                "1. Clean up: remove any non-physical-asset entries (events, aggregates, "
                "datasets) and standardize messy asset_type_raw values.\n"
                "2. Evaluate coverage against the company profile.\n"
                "3. Fill gaps if needed — rag_query to verify, rag_extract or "
                "scrape_and_extract for new data.\n"
                "Return QAReport with summary."
            )
            if costs:
                costs.track_pydantic_ai(result.usage(), config.qa_model, "qa")
            qa_report = result.output
            if not qa_report.should_enrich:
                break
            asset_summary = _build_asset_summary(assets)

    return qa_report


def _build_asset_summary(assets: list[Asset]) -> str:
    by_type: dict[str, int] = {}
    for a in assets:
        t = a.naturesense_asset_type or a.asset_type_raw or "Unknown"
        by_type[t] = by_type.get(t, 0) + 1
    lines = [f"Total: {len(assets)} assets"]
    if by_type:
        lines.append(
            "By type: " + ", ".join(
                f"{t}: {c}" for t, c in sorted(by_type.items(), key=lambda x: -x[1])
            )
        )
    return "\n".join(lines)
