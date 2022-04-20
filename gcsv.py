import os
from typing import Any, List
import csv

from db import ColumnInfo, DBTable


def create_columns(table: DBTable, headers: List[str]) -> None:
    if len(headers) < 1:
        raise ValueError("CSV: Invalid headers")
    first_header = headers[0]
    # Assume first column is pk
    table.add_column(first_header, ColumnInfo(primary_key=True))
    for header in headers[1:]:
        table.add_column(name=header, col=ColumnInfo())


def insert_rows(table: DBTable, headers: List[str], reader: Any) -> int:
    count = 0
    for row in reader:
        to_insert = {}
        for val, header in zip(row, headers):
            if val.isnumeric():
                val = int(val)
            to_insert[header] = val
        table.insert(to_insert)

        count += 1
    return count


def run_csv(
    file_path: str,
    delimiter: str = ",",
    table_name: str = "default_table",
    dbpath: str = "default_db",
) -> None:
    if not table_name:
        print("Table name not specified, defaulting to `default_table`")
        table_name = "default_table"
    if table_name.lower() == "table":
        raise ValueError(
            "'table' is a reserved keyword. Please use another name for the table you wish to create."
        )

    print(f'Parsing CSV "{file_path}"...')
    with open(file_path) as f:
        reader = csv.reader(f, delimiter=delimiter)

        dbexists = True

        dbdir = os.path.join(os.curdir, dbpath)
        if not os.path.isdir(dbdir):
            os.mkdir(dbdir)
            dbexists = False
        table = DBTable(name=table_name, path=dbdir)

        headers = next(reader)
        if not dbexists:
            create_columns(table, headers)

        rows_inserted = insert_rows(table, headers, reader)

        table.save()

        print(f"Parsing finished, {rows_inserted} rows inserted.")
