#!/usr/bin/env python3

import argparse

from interactive import run_interactive
from gcsv import run_csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interactive", help="runs GatorDB in interactive mode", action="store_true"
    )
    parser.add_argument(
        "--csv", help="parses CSV into GatorDB table; please specify csv file path."
    )
    parser.add_argument("--csv-name", help="CSV: name of table to parse CSV into")
    parser.add_argument("--delimiter", help="CSV: delimiter", default=",")
    parser.add_argument("--dbpath", help="path to store db")

    args = parser.parse_args()

    if args.interactive:
        run_interactive(args)
    if args.csv:
        run_csv(args.csv, args.delimiter, args.csv_name, args.dbpath)
