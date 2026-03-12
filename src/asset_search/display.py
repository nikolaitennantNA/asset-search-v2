"""Display layer -- Rich terminal output for the pipeline.

Provides styled panels, stage headers, progress bars, and summary tables.
Ported and simplified from asset-search v1 display.py.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

console = Console()

TOTAL_STAGES = 6


def show_stage(stage: int, label: str) -> None:
    """Print a bold stage header like '[3/7] Crawling & ingesting...'"""
    console.print(
        f"  [bold cyan][{stage}/{TOTAL_STAGES}][/bold cyan] {label}"
    )


def show_detail(msg: str) -> None:
    """Print an indented dim detail line."""
    console.print(f"        [dim]{msg}[/dim]")


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
    isin: str,
    profile=None,
    website: str = "",
    description: str = "",
) -> None:
    """Display the styled intro box with company info."""
    lines = [f"[bold]ISIN:[/bold] [dim]{isin}[/dim]"]
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
        task = progress.add_task(f"  {label}", total=total)
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
    table.add_column("Name", style="cyan", max_width=40)
    table.add_column("Type", style="green")
    table.add_column("Entity", style="white")
    table.add_column("Address", style="white", max_width=30)
    table.add_column("Coords", style="dim")

    for asset in assets[:max_rows]:
        name = _asset_field(asset, "asset_name")
        atype = _asset_field(asset, "asset_type_raw", "asset_type")
        entity = _asset_field(asset, "entity_name")
        address = _asset_field(asset, "address")
        coords = ""
        lat_str = _asset_field(asset, "latitude")
        lon_str = _asset_field(asset, "longitude")
        try:
            if lat_str and lon_str:
                coords = f"{float(lat_str):.4f}, {float(lon_str):.4f}"
        except (ValueError, TypeError):
            pass
        table.add_row(name[:40], atype, entity, address[:30], coords)

    if len(assets) > max_rows:
        table.add_row("...", f"+{len(assets) - max_rows} more", "", "", "")

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
        if costs.crawl4ai_pages:
            table.add_row("Crawl4AI pages", str(costs.crawl4ai_pages))
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
