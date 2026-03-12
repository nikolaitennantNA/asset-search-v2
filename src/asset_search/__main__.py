"""CLI entry point: python -m asset_search run <ISIN>"""

from __future__ import annotations

import argparse
import asyncio
import sys

from .config import Config
from .pipeline import run


def main():
    parser = argparse.ArgumentParser(description="Asset Search v2")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run pipeline for a company")
    run_parser.add_argument("isin", help="Company ISIN or issuer_id")
    run_parser.add_argument(
        "--stop-after",
        choices=["profile", "discover", "scrape", "extract", "merge", "qa"],
        help="Stop after this stage",
    )

    args = parser.parse_args()

    if args.command == "run":
        config = Config()
        result = asyncio.run(run(args.isin, config, stop_after=args.stop_after))
        print(f"\nDone. {result['asset_count']} assets in {result['elapsed']:.1f}s")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
