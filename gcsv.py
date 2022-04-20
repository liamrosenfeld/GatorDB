import os
from typing import Any, List
import csv

from db import ColumnInfo, DBTable, DBType


def create_columns(table: DBTable, headers: List[str], reader: Any) -> None:
    if len(headers) < 1:
        raise ValueError("CSV: Invalid headers")

    # Infer type from first row
    first_row: List[str] = next(reader)
    first_row_isnumerics = [val.isnumeric() for val in first_row]
    first_row_types = [
        DBType.INTEGER if val else DBType.STRING for val in first_row_isnumerics
    ]

    first_header = headers[0]
    # Assume first column is pk
    table.add_column(first_header, ColumnInfo(primary_key=True))
    for i, header in enumerate(headers[1:]):
        table.add_column(
            name=header,
            col=ColumnInfo(dbtype=first_row_types[i + 1], primary_key=False),
        )


def insert_rows(table: DBTable, headers: List[str], reader: Any) -> int:
    count = 0
    for row in reader:
        # Ignore blank rows
        if not row:
            continue

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

        table_exists = False

        dbdir = os.path.join(os.curdir, dbpath)
        if not os.path.isdir(dbdir):
            os.mkdir(dbdir)
        if os.path.isdir("/".join([dbdir, table_name])):
            table_exists = True

        table = DBTable(name=table_name, path=dbdir)

        headers: List[str] = next(reader)

        if not table_exists:
            create_columns(table, headers, reader)

        rows_inserted = insert_rows(table, headers, reader) + 1

        table.save()

        print(f"Parsing finished, {rows_inserted} rows inserted.")
