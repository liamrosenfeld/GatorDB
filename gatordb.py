#!/usr/bin/env python3

import argparse

from interactive import run_interactive

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interactive", help="runs GatorDB in interactive mode", action="store_true"
    )

    args = parser.parse_args()

    if args.interactive:
        run_interactive()
