from typing import Any, List
import csv

from db import ColumnInfo, DBTable, DBType
from query import Change, Condition, ConditionType


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


def run_csv(file_path: str, delimiter: str = ",", autocreate: bool = False) -> None:
    print(f'Parsing CSV "{file_path}"...')
    with open(file_path) as f:
        reader = csv.reader(f, delimiter=delimiter)

        table = DBTable()

        headers = next(reader)
        if autocreate:
            create_columns(table, headers)

        rows_inserted = insert_rows(table, headers, reader)

        print(f"Parsing finished, {rows_inserted} rows inserted.")
