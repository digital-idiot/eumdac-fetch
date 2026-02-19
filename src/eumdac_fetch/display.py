"""Rich display utilities for console output."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from eumdac_fetch.search import CollectionInfo

console = Console()


def display_collection_info(info: CollectionInfo) -> None:
    """Display collection details in a Rich panel."""
    console.print()
    console.print(f"[bold cyan]Collection:[/] {info.collection_id}")
    console.print(f"[bold]Title:[/] {info.title}")
    console.print(f"[bold]Abstract:[/] {info.abstract}")

    if info.search_options:
        console.print()
        console.print("[bold]Available Search Filters:[/]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Filter")
        table.add_column("Options / Type")
        for key, value in sorted(info.search_options.items()):
            if isinstance(value, list):
                table.add_row(key, ", ".join(str(v) for v in value))
            else:
                table.add_row(key, str(value))
        console.print(table)
    console.print()


def display_search_results(products: list, total: int, filters_used: dict) -> None:
    """Display search results as a Rich table."""
    console.print()
    console.print(f"[bold]Total matching products:[/] {total}")
    console.print(f"[bold]Showing:[/] {len(products)}")
    if filters_used:
        console.print(f"[dim]Filters: {filters_used}[/dim]")
    console.print()

    if not products:
        console.print("[yellow]No products found.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", justify="right", style="dim")
    table.add_column("Product ID")
    table.add_column("Size (KB)", justify="right")

    for i, product in enumerate(products, 1):
        product_id = str(product)
        product_size = getattr(product, 'size', None)
        product_size = f"{product_size:,.0f}" if isinstance(product_size, (int, float)) else "N/A"
        table.add_row(str(i), product_id, product_size)

    console.print(table)
    console.print()


def display_product_count(collection_id: str, count: int) -> None:
    """Display just the product count."""
    console.print(f"[bold]{collection_id}:[/] {count:,} products matching filters")
