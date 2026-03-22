"""Run the TinyDetective backend with `python -m backend`."""

from __future__ import annotations

import argparse
import os


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m backend")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Replay matching completed saved investigations instead of running the live pipeline.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.demo:
        os.environ["TINYDETECTIVE_DEMO_MODE"] = "1"

    from backend.main import run as main

    main()
