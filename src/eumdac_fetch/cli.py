"""Click CLI entry point for eumdac-fetch."""

from __future__ import annotations

import sys

import click

from eumdac_fetch import __version__


@click.group()
@click.version_option(version=__version__)
def cli():
    """eumdac-fetch: Bulk EUMETSAT data downloader."""


@cli.command()
@click.option("--key", envvar="EUMDAC_KEY", help="EUMDAC API key")
@click.option("--secret", envvar="EUMDAC_SECRET", help="EUMDAC API secret")
@click.option("--validity", type=int, default=None, help="Token validity in seconds (default: 86400)")
def collections(key: str | None, secret: str | None, validity: int | None):
    """List all available EUMETSAT collections."""
    import eumdac

    from eumdac_fetch.search import SearchService

    if not key or not secret:
        click.echo("Error: API credentials required. Set EUMDAC_KEY/EUMDAC_SECRET or use --key/--secret.", err=True)
        sys.exit(1)

    try:
        token_kwargs = {"validity": validity} if validity is not None else {}
        token = eumdac.AccessToken(credentials=(key, secret), **token_kwargs)
        service = SearchService(token)
        collections_list = service.list_collections()

        click.echo(f"\nFound {len(collections_list)} collections:\n")
        for coll in collections_list:
            click.echo(f"{coll.collection_id}")
            click.echo(f"  {coll.title}\n")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("collection_id")
@click.option("--key", envvar="EUMDAC_KEY", help="EUMDAC API key")
@click.option("--secret", envvar="EUMDAC_SECRET", help="EUMDAC API secret")
@click.option("--validity", type=int, default=None, help="Token validity in seconds (default: 86400)")
def info(collection_id: str, key: str | None, secret: str | None, validity: int | None):
    """Show collection details and available search filters."""
    import eumdac

    from eumdac_fetch.display import display_collection_info
    from eumdac_fetch.search import SearchService

    if not key or not secret:
        click.echo("Error: API credentials required. Set EUMDAC_KEY/EUMDAC_SECRET or use --key/--secret.", err=True)
        sys.exit(1)

    try:
        token_kwargs = {"validity": validity} if validity is not None else {}
        token = eumdac.AccessToken(credentials=(key, secret), **token_kwargs)
        service = SearchService(token)
        collection_info = service.get_collection_info(collection_id)
        display_collection_info(collection_info)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True), help="Job config YAML")
@click.option("--limit", default=50, help="Max products to show per job")
@click.option("--count-only", is_flag=True, help="Only show product counts")
@click.option("--validity", type=int, default=None, help="Token validity in seconds (default: from ENV or 86400)")
def search(config_path: str, limit: int, count_only: bool, validity: int | None):
    """Search and list matching products (dry run)."""
    from eumdac_fetch.auth import get_token
    from eumdac_fetch.config import load_config
    from eumdac_fetch.display import console, display_product_count, display_search_results
    from eumdac_fetch.env import ENV
    from eumdac_fetch.logging_config import setup_logging
    from eumdac_fetch.search import SearchService

    try:
        app_config = load_config(config_path)
        setup_logging(app_config.logging)
        if validity is not None:
            ENV.validity = validity
        token = get_token()
        service = SearchService(token)

        for job in app_config.jobs:
            console.print(f"\n[bold cyan]Job: {job.name}[/]")
            console.print(f"Collection: {job.collection}")

            if count_only:
                count = service.count(job.collection, job.filters)
                display_product_count(job.collection, count)
            else:
                result = service.search(job.collection, job.filters, limit=limit)
                display_search_results(result.products, result.total, result.filters_used)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True), help="Job config YAML")
@click.option("--validity", type=int, default=None, help="Token validity in seconds (default: from ENV or 86400)")
def download(config_path: str, validity: int | None):
    """Download products defined in job config."""
    import asyncio

    from eumdac_fetch.auth import get_token
    from eumdac_fetch.config import load_config
    from eumdac_fetch.display import console
    from eumdac_fetch.downloader import DownloadService
    from eumdac_fetch.env import ENV
    from eumdac_fetch.logging_config import add_session_log_handler, setup_logging
    from eumdac_fetch.search import SearchService
    from eumdac_fetch.session import Session
    from eumdac_fetch.state import StateDB

    try:
        app_config = load_config(config_path)
        setup_logging(app_config.logging)
        if validity is not None:
            ENV.validity = validity
        token = get_token()
        search_service = SearchService(token)

        for job in app_config.jobs:
            # Create session
            session = Session(job)
            session.initialize()

            status = "new" if session.is_new else "resuming"
            console.print(f"\n[bold cyan]Session: {session.session_id} ({status})[/]")
            console.print(f"Session dir: {session.session_dir}")
            if session.is_live:
                console.print("[yellow]Live session — search results will be refreshed[/]")

            console.print(f"\n[bold cyan]Job: {job.name}[/]")

            # Set up session logging and state DB
            log_handler = add_session_log_handler(session.log_path)
            state_db = StateDB(session.state_db_path)

            try:
                # Reset stale DOWNLOADING products from previous killed runs
                if not session.is_new:
                    reset_count = state_db.reset_stale_downloads(job.name)
                    if reset_count:
                        console.print(f"Reset {reset_count} stale downloading products to pending")

                # Search with caching
                if not session.is_new and not session.is_live and state_db.has_cached_search():
                    console.print("Using cached search results...")
                    resumable = state_db.get_resumable(job.name)
                    if not resumable:
                        console.print("[green]All products already downloaded.[/]")
                        continue
                    console.print(f"Re-fetching {len(resumable)} resumable products...")
                    products = search_service.iter_products(job.collection, job.filters, limit=job.limit)
                    resumable_ids = {r.product_id for r in resumable}
                    products = [p for p in products if str(p) in resumable_ids]
                else:
                    console.print("Searching for products...")
                    products = search_service.iter_products(job.collection, job.filters, limit=job.limit)
                    if products:
                        state_db.cache_search_results(products, job.collection)

                console.print(f"Found {len(products)} products")

                if not products:
                    continue

                # Download using session download dir
                download_service = DownloadService(
                    state_db=state_db,
                    download_dir=session.download_dir,
                    parallel=job.download.parallel,
                    resume=job.download.resume,
                    verify_md5=job.download.verify_md5,
                    max_retries=job.download.max_retries,
                    retry_backoff=job.download.retry_backoff,
                    timeout=job.download.timeout,
                )

                asyncio.run(download_service.download_all(products, job.name, job.collection))
                console.print(f"[green]Download complete for job: {job.name}[/]")
            finally:
                state_db.close()
                import logging

                logging.getLogger("eumdac_fetch").removeHandler(log_handler)

    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted.[/yellow]")
        sys.exit(130)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True), help="Job config YAML")
@click.option(
    "--post-processor",
    "post_processor_path",
    default=None,
    help="Post-processor callable as 'module:function' (e.g. 'mymodule:my_func')",
)
@click.option("--validity", type=int, default=None, help="Token validity in seconds (default: from ENV or 86400)")
def run(config_path: str, post_processor_path: str | None, validity: int | None):
    """Daemon mode: search + download + post-process pipeline."""
    import asyncio
    import importlib

    from eumdac_fetch.auth import get_token
    from eumdac_fetch.config import load_config
    from eumdac_fetch.display import console
    from eumdac_fetch.env import ENV
    from eumdac_fetch.logging_config import setup_logging
    from eumdac_fetch.pipeline import Pipeline

    try:
        app_config = load_config(config_path)
        setup_logging(app_config.logging)
        if validity is not None:
            ENV.validity = validity
        token = get_token()

        post_processor = None
        if post_processor_path:
            module_path, _, func_name = post_processor_path.partition(":")
            if not func_name:
                click.echo(f"Error: --post-processor must be 'module:function', got '{post_processor_path}'", err=True)
                sys.exit(1)
            module = importlib.import_module(module_path)
            post_processor = getattr(module, func_name)

        pipeline = Pipeline(token=token, config=app_config, post_processor=post_processor)
        console.print("[bold]Starting pipeline...[/]")
        asyncio.run(pipeline.run())
        console.print("[green]Pipeline complete.[/]")

    except KeyboardInterrupt:
        console.print("\n[yellow]Pipeline shutting down...[/yellow]")
        sys.exit(130)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
