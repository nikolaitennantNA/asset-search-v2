"""CLI entry point: python -m asset_search run <ISIN>"""

from __future__ import annotations

import argparse
import asyncio
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="asset_search",
        description="Web-based physical asset discovery pipeline",
    )
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Run asset discovery pipeline")
    run_parser.add_argument(
        "isin",
        nargs="?",
        default=None,
        help="ISIN identifier for the target company",
    )
    run_parser.add_argument(
        "--portfolio",
        action="store_true",
        help="Run in batch mode across the portfolio",
    )
    run_parser.add_argument(
        "--max-companies",
        type=int,
        default=5,
        help="Max companies to process in batch mode (default: 5)",
    )

    args = parser.parse_args()

    if args.command != "run":
        parser.print_help()
        sys.exit(1)

    if args.portfolio:
        print(
            f"Batch mode: processing up to {args.max_companies} companies"
        )
        raise NotImplementedError("Batch mode not yet implemented")

    if not args.isin:
        run_parser.error("ISIN is required (or use --portfolio)")

    from .config import Config
    from .pipeline import run

    config = Config()
    asyncio.run(run(args.isin, config))


if __name__ == "__main__":
    main()
