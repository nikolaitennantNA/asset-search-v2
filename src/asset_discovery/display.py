"""Display layer -- Rich terminal output for the pipeline.

Provides styled panels, stage headers, progress bars, and summary tables.
Ported and simplified from asset-discovery v1 display.py.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncGenerator, Generator

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

console = Console()

TOTAL_STAGES = 6


def show_stage(stage: int, label: str) -> None:
    """Print a bold stage header like '[3/6] Crawling & ingesting...'"""
    console.print(
        f"  [bold cyan][{stage}/{TOTAL_STAGES}][/bold cyan] {label}"
    )


def show_detail(msg: str) -> None:
    """Print an indented dim detail line."""
    console.print(f"        [dim]{msg}[/dim]")


@contextmanager
def show_spinner(label: str) -> Generator[None, None, None]:
    """Show an animated spinner with a label. Clears when done."""
    with Live(
        Spinner("dots", text=f"[dim]        {label}[/dim]"),
        console=console,
        transient=True,
    ):
        yield


def show_success(msg: str) -> None:
    """Print a green success detail."""
    console.print(f"        [green]{msg}[/green]")


def show_warning(msg: str) -> None:
    """Print a yellow warning."""
    console.print(f"        [yellow]{msg}[/yellow]")


def show_error(msg: str) -> None:
    """Print a red error."""
    console.print(f"        [red]ERROR: {msg}[/red]")


def show_intro_panel(
    company_name: str,
    issuer_id: str,
    profile=None,
    website: str = "",
    description: str = "",
) -> None:
    """Display the styled intro box with company info."""
    lines = [f"[bold]Issuer ID:[/bold] [dim]{issuer_id}[/dim]"]
    if profile is not None:
        if hasattr(profile, "jurisdiction") and profile.jurisdiction:
            lines.append(f"[bold]Jurisdiction:[/bold] [dim]{profile.jurisdiction}[/dim]")
        if hasattr(profile, "primary_industry") and profile.primary_industry:
            lines.append(f"[bold]Industry:[/bold] [dim]{profile.primary_industry}[/dim]")
        if hasattr(profile, "estimated_asset_count") and profile.estimated_asset_count:
            lines.append(f"[bold]Estimated assets:[/bold] [dim]~{profile.estimated_asset_count}[/dim]")
        if hasattr(profile, "subsidiaries"):
            lines.append(f"[bold]Subsidiaries:[/bold] [dim]{len(profile.subsidiaries)}[/dim]")
        if hasattr(profile, "description") and profile.description and not description:
            description = profile.description
    if description:
        lines.append(f"[bold]Description:[/bold] [dim]{description}[/dim]")
    if website:
        lines.append(f"[bold]Website:[/bold] [dim]{website}[/dim]")
    console.print()
    console.print(
        Panel(
            "\n".join(lines),
            title=f"[bold cyan]{company_name}[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )


@contextmanager
def stage_progress(
    total: int, label: str = "Processing", unit: str = "items"
) -> Generator[Any, None, None]:
    """Context manager yielding a Rich Progress task.

    Usage:
        with stage_progress(47, "Crawling", "pages") as (progress, task):
            for url in urls:
                await crawl(url)
                progress.advance(task)
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn(f"{{task.completed}}/{{task.total}} {unit}"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(f"   {label}", total=total)
        yield progress, task


def _asset_field(asset: Any, *keys: str, default: str = "") -> str:
    """Get a field from an asset that may be a dict or a model object."""
    for key in keys:
        if isinstance(asset, dict):
            val = asset.get(key)
        else:
            val = getattr(asset, key, None)
        if val is not None:
            return str(val)
    return default


def show_assets_table(
    assets: list[Any], max_rows: int = 20
) -> None:
    """Display a compact table of discovered assets (dicts or model objects)."""
    if not assets:
        console.print("[dim]No assets found.[/dim]")
        return

    table = Table(
        title=f"[bold]Found {len(assets)} Assets[/bold]",
        border_style="dim",
    )
    table.add_column("Asset Name", style="cyan", max_width=40)
    table.add_column("Asset Type", style="green")
    table.add_column("Entity Name", style="white")
    table.add_column("Address", style="white", max_width=30)
    table.add_column("Lat, Lon", style="dim")
    table.add_column("Status", style="dim")

    for asset in assets[:max_rows]:
        name = _asset_field(asset, "asset_name")
        atype = _asset_field(asset, "asset_type_raw", "asset_type")
        entity = _asset_field(asset, "entity_name")
        address = _asset_field(asset, "address")
        status = _asset_field(asset, "status")
        coords = ""
        lat_str = _asset_field(asset, "latitude")
        lon_str = _asset_field(asset, "longitude")
        try:
            if lat_str and lon_str:
                coords = f"{float(lat_str):.4f}, {float(lon_str):.4f}"
        except (ValueError, TypeError):
            pass
        table.add_row(name[:40], atype, entity, address[:30], coords, status)

    if len(assets) > max_rows:
        table.add_row("...", f"+{len(assets) - max_rows} more", "", "", "")

    console.print(table)


def show_coverage_flags(qa_report: Any) -> None:
    """Display QA coverage flags if any exist."""
    if qa_report is None:
        return
    flags = getattr(qa_report, "coverage_flags", None) or []
    if not flags:
        return
    severity_style = {"high": "red", "medium": "yellow", "low": "dim"}
    table = Table(
        title="[bold]Coverage Flags[/bold]",
        border_style="yellow",
    )
    table.add_column("Severity", style="bold")
    table.add_column("Type")
    table.add_column("Description", max_width=60)
    for flag in flags:
        sev = getattr(flag, "severity", "medium")
        style = severity_style.get(sev, "white")
        table.add_row(
            f"[{style}]{sev}[/{style}]",
            getattr(flag, "flag_type", ""),
            getattr(flag, "description", ""),
        )
    console.print(table)


def show_cost_summary(
    stages_run: list[str] | None = None,
    url_count: int = 0,
    page_count: int = 0,
    asset_count: int = 0,
    elapsed: float = 0.0,
    costs: Any = None,
) -> None:
    """Display a cost/usage summary table with optional CostTracker breakdown."""
    table = Table(
        title="[bold]Pipeline Summary[/bold]",
        show_header=False,
        padding=(0, 2),
        border_style="cyan",
    )
    table.add_column("Label", style="bold")
    table.add_column("Value")

    if stages_run:
        table.add_row("Stages", " -> ".join(stages_run))
    if url_count:
        table.add_row("URLs discovered", str(url_count))
    table.add_row("Pages scraped", str(page_count))
    table.add_row("Assets found", str(asset_count))

    if costs is not None:
        total_tokens = costs.total_input_tokens + costs.total_output_tokens
        if total_tokens > 0:
            tok_str = f"{total_tokens / 1000:.1f}k" if total_tokens >= 1000 else str(total_tokens)
            table.add_row("Tokens (in/out)", f"{costs.total_input_tokens:,} / {costs.total_output_tokens:,} ({tok_str} total)")
        if costs.spider_pages:
            table.add_row("Spider pages", f"{costs.spider_pages} (${costs.spider_cost_usd:.4f})")
        if costs.exa_searches:
            table.add_row("Exa searches", str(costs.exa_searches))
        if costs.cohere_rerank_calls:
            table.add_row("Cohere reranks", str(costs.cohere_rerank_calls))
        cost_usd = costs.total_cost_usd()
        cost_gbp = costs.total_cost_gbp()
        if cost_usd > 0:
            table.add_row("Cost (est.)", f"${cost_usd:.2f} / \u00a3{cost_gbp:.2f}")

    if elapsed > 0:
        mins, secs = divmod(int(elapsed), 60)
        if mins:
            table.add_row("Duration", f"{mins}m {secs:02d}s")
        else:
            table.add_row("Duration", f"{secs}s")

    console.print(table)


# ── Discover stage display ──────────────────────────────────────────────────


class DiscoverDisplay:
    """Tree-style live display for the discover stage.

    Tool events come from tools.py via the on_event callback.
    Agent text and web search events come from discover.py.

    Uses tree connectors (├─ / └─) under each domain, with web searches
    as visual section separators and Rich Panel for header/footer.
    """

    _LABEL_W = 9   # action label column width
    _IND = "   "    # base indent (3 spaces)
    _PAD = 3        # left padding for Padding objects

    def __init__(self, company_name: str = ""):
        self._current_domain: str | None = None
        self._seen_domains: set[str] = set()
        self._plan_shown = False
        self._total_saved = 0
        self._company_name = company_name
        self._start = time.monotonic()
        # Buffered detail line — printed with ├─ when next arrives,
        # or └─ when domain/section ends.
        self._pending: tuple[str, str] | None = None  # (msg, style)
        # Buffer web searches that arrive before the plan text
        self._pre_plan_searches: list[str] = []

    def show_header(self) -> None:
        """Print panel header with stage number and company name."""
        title = Text()
        title.append("[2/6]", style="bold cyan")
        title.append(" Discovering URLs", style="bold")
        if self._company_name:
            title.append("  ·  ", style="dim")
            title.append(self._company_name)
        console.print(Panel(title, border_style="dim", padding=(0, 1)))

    # ── buffered tree connectors ─────────────────────────────────

    def _flush(self, is_last: bool = False) -> None:
        """Print the pending detail line with the correct tree connector."""
        if self._pending is None:
            return
        msg, style = self._pending
        connector = "└─" if is_last else "├─"
        t = Text(f"{connector} ")
        t.append(msg, style=style)
        from rich.padding import Padding
        console.print(Padding(t, (0, 0, 0, self._PAD)))
        self._pending = None

    def _queue(self, msg: str, style: str = "dim") -> None:
        """Queue a detail line. Previous pending is flushed with ├─."""
        self._flush(is_last=False)
        self._pending = (msg, style)

    def _end_section(self) -> None:
        """Close current domain section — flush pending with └─."""
        self._flush(is_last=True)

    # ── domain / section management ──────────────────────────────

    def _start_domain(self, domain: str) -> None:
        """Start a new domain section with a bold header."""
        self._end_section()
        self._current_domain = domain
        first = domain not in self._seen_domains
        self._seen_domains.add(domain)
        console.print()
        t = Text()
        t.append(domain, style="bold" if first else "dim")
        from rich.padding import Padding
        console.print(Padding(t, (0, 0, 0, self._PAD)))

    # ── public event handlers ────────────────────────────────────

    def _show_plan(self, text: str) -> None:
        """Show the plan, then flush any buffered pre-plan web searches."""
        self._plan_shown = True
        for prefix in ("Approach:", "Plan:", "Strategy:"):
            if text.startswith(prefix):
                text = text[len(prefix) :].strip()
                break
        self._end_section()
        console.print()
        t = Text()
        t.append("Plan: ", style="bold dim")
        t.append(text, style="italic dim")
        from rich.padding import Padding
        console.print(Padding(t, (0, 0, 0, self._PAD)))
        # Now flush any web searches that arrived before the plan
        for q in self._pre_plan_searches:
            self._show_web_search(q)
        self._pre_plan_searches.clear()

    def _show_web_search(self, query: str) -> None:
        """Actually print a web search line."""
        self._end_section()
        if len(query) > 80:
            query = query[:77] + "..."
        console.print()
        t = Text()
        t.append(f'web_search "{query}"', style="cyan")
        from rich.padding import Padding
        console.print(Padding(t, (0, 0, 0, self._PAD)))

    def on_agent_text(self, text: str) -> None:
        """Handle text from the agent. First text becomes the plan line.
        Short text stays in the tree. Long text (final summary) breaks out."""
        text = text.strip()
        if not text:
            return
        if not self._plan_shown:
            self._show_plan(text)
        elif len(text) > 200:
            # Long text = final summary. Break out of the tree.
            self._end_section()
            console.print()
            t = Text()
            t.append(text, style="italic dim")
            from rich.padding import Padding
            console.print(Padding(t, (0, 0, 0, self._PAD)))
        else:
            # Short note stays inside the current domain's tree
            self._queue(text, style="italic dim")

    def on_web_search(self, query: str) -> None:
        """Display a web search call. Buffers if plan hasn't been shown yet."""
        if not self._plan_shown:
            self._pre_plan_searches.append(query)
            return
        self._show_web_search(query)

    def _flush_pre_plan(self) -> None:
        """If plan never arrived, flush buffered web searches before tool events."""
        if not self._plan_shown and self._pre_plan_searches:
            self._plan_shown = True  # skip plan display
            for q in self._pre_plan_searches:
                self._show_web_search(q)
            self._pre_plan_searches.clear()

    def on_event(self, event: str, data: dict) -> None:
        """Handle a tool result event from tools.py."""
        self._flush_pre_plan()
        domain = data.get("domain", "")
        if domain and domain != self._current_domain:
            self._start_domain(domain)

        if event == "sitemap_indexes":
            n = data.get("count", 0)
            self._queue(f"{'sitemap':<{self._LABEL_W}}{n} indexes found")

        elif event == "sitemap_urls":
            n = data.get("count", 0)
            name = data.get("sitemap", "")
            if name:
                self._queue(f"{'sitemap':<{self._LABEL_W}}{name} → {n} urls")
            else:
                self._queue(f"{'sitemap':<{self._LABEL_W}}{n} urls")

        elif event == "crawl_result":
            path = data.get("path", "/")
            if len(path) > 50:
                path = path[:47] + "..."
            success = data.get("success", True)
            label = path if success else f"{path} (failed)"
            self._queue(f"{'crawl':<{self._LABEL_W}}{label}")

        elif event == "probe_result":
            total = data.get("total", 0)
            exist = data.get("exist", 0)
            paths = data.get("paths", [])
            if paths:
                # Show short path names — truncate long slugs
                short = []
                for p in paths[:6]:
                    p = p.rstrip("/")
                    name = p.rsplit("/", 1)[-1] if "/" in p else p
                    if len(name) > 20:
                        name = name[:17] + "..."
                    short.append(name)
                shown = ", ".join(short)
                if len(paths) > 6:
                    shown += f" +{len(paths) - 6}"
                self._queue(f"{'probe':<{self._LABEL_W}}{exist}/{total} exist ({shown})")
            else:
                self._queue(f"{'probe':<{self._LABEL_W}}{total} paths → {exist} exist")

        elif event == "save_result":
            count = data.get("count", 0)
            self._total_saved += count
            self._queue(f"{'save':<{self._LABEL_W}}{count} urls", style="green")

        elif event == "map_result":
            count = data.get("count", 0)
            self._queue(f"{'mapped':<{self._LABEL_W}}{count} urls")

        elif event == "spider_result":
            count = data.get("count", 0)
            self._queue(f"{'spider':<{self._LABEL_W}}{count} links")

        elif event == "bulk_save_empty":
            total = data.get("total_sitemap", 0)
            inc = data.get("include")
            exc = data.get("exclude")
            filt = f"include={inc}" if inc else f"exclude={exc}" if exc else "no filter"
            self._queue(
                f"{'save':<{self._LABEL_W}}0 urls (bulk: {total} sitemap, {filt})",
                style="yellow",
            )

        elif event == "remove_result":
            count = data.get("count", 0)
            self._end_section()
            console.print()
            t = Text()
            t.append(f"pruned {count} urls", style="yellow")
            from rich.padding import Padding
            console.print(Padding(t, (0, 0, 0, self._PAD)))

    def show_footer(self, url_count: int | None = None) -> None:
        """Print summary line with total URLs and elapsed time."""
        self._end_section()
        elapsed = time.monotonic() - self._start
        count = url_count if url_count is not None else self._total_saved
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"
        console.print()
        t = Text()
        t.append(f"  Done  ·  {count} urls saved  ·  {time_str}", style="bold green")
        console.print(t)
        console.print()  # space before next stage
