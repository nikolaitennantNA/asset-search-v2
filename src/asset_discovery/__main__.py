"""CLI entry point: python -m asset_discovery run <ISIN>"""

from __future__ import annotations

import argparse
import asyncio
import sys

from .config import Config
from .pipeline import run


def main():
    parser = argparse.ArgumentParser(description="Asset Discovery")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run pipeline for a company")
    run_parser.add_argument("isin", nargs="?", help="Company ISIN or issuer_id")
    run_parser.add_argument(
        "--from-file", "-f",
        help="Load company profile from JSON file instead of Postgres",
    )
    run_parser.add_argument(
        "--stop-after",
        choices=["profile", "discover", "scrape", "extract", "merge", "qa"],
        help="Stop after this stage",
    )
    run_parser.add_argument(
        "--start-from",
        choices=["discover", "scrape", "extract", "merge", "qa"],
        help="Resume from this stage, loading prior results from DB/cache",
    )
    run_parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show tool calls, search queries, and LLM interactions",
    )
    run_parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Skip all DB caches (scrape, extraction) — re-run everything fresh",
    )

    args = parser.parse_args()

    if args.command == "run":
        if not args.isin and not args.from_file:
            run_parser.error("provide an ISIN or --from-file")
        config = Config()
        if args.verbose:
            import logging
            logging.basicConfig(level=logging.DEBUG, format="  %(name)s: %(message)s")
        result = asyncio.run(run(
            args.isin,
            config,
            stop_after=args.stop_after,
            start_from=args.start_from,
            profile_file=args.from_file,
            verbose=args.verbose,
            no_cache=args.no_cache,
        ))
        print(f"\nDone. {result['asset_count']} assets in {result['elapsed']:.1f}s")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
