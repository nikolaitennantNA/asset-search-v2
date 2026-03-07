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

TOTAL_STAGES = 7


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
    website: str = "",
    description: str = "",
) -> None:
    """Display the styled intro box with company info."""
    lines = [f"[bold]ISIN:[/bold] [dim]{isin}[/dim]"]
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


def show_assets_table(
    assets: list[dict[str, Any]], max_rows: int = 20
) -> None:
    """Display a compact table of discovered assets."""
    if not assets:
        return

    table = Table(
        title=f"[bold]Found {len(assets)} Assets[/bold]",
        border_style="dim",
    )
    table.add_column("Name", style="cyan", max_width=40)
    table.add_column("Type", style="green")
    table.add_column("Location", style="white", max_width=40)
    table.add_column("Coords", style="dim")

    for asset in assets[:max_rows]:
        address = asset.get("address", "")
        coords = ""
        lat, lon = asset.get("latitude"), asset.get("longitude")
        if lat is not None and lon is not None:
            coords = f"{lat:.4f}, {lon:.4f}"
        table.add_row(
            (asset.get("asset_name", "") or "")[:40],
            asset.get("asset_type", ""),
            address[:40],
            coords,
        )

    if len(assets) > max_rows:
        table.add_row("...", f"+{len(assets) - max_rows} more", "", "")

    console.print(table)


def show_cost_summary(
    total_cost_usd: float = 0.0,
    total_tokens: int = 0,
    pages_crawled: int = 0,
    assets_found: int = 0,
    elapsed_seconds: float = 0.0,
) -> None:
    """Display a cost/usage summary table."""
    table = Table(
        title="[bold]Pipeline Summary[/bold]",
        show_header=False,
        padding=(0, 2),
        border_style="cyan",
    )
    table.add_column("Label", style="bold")
    table.add_column("Value")

    table.add_row("Assets found", str(assets_found))
    table.add_row("Pages crawled", str(pages_crawled))

    if total_tokens > 0:
        tok_str = (
            f"{total_tokens / 1000:.1f}k"
            if total_tokens >= 1000
            else str(total_tokens)
        )
        table.add_row("Tokens", tok_str)

    if total_cost_usd > 0:
        table.add_row("Cost (est.)", f"${total_cost_usd:.2f}")

    if elapsed_seconds > 0:
        mins, secs = divmod(int(elapsed_seconds), 60)
        if mins:
            table.add_row("Duration", f"{mins}m {secs:02d}s")
        else:
            table.add_row("Duration", f"{secs}s")

    console.print(table)
