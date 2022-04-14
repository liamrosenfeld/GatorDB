from sqlengine import SQLEngine


def create_table():
    pass


def insert_rows():
    pass


def delete_rows():
    pass


def drop_table():
    pass


COMMANDS = {
    "CREATE_TABLE": create_table,
    "HATCH": create_table,
    "INSERT_INTO": insert_rows,
    "SHOVE": insert_rows,
    "DELETE": delete_rows,
    "CHOMP": delete_rows,
    "DROP_TABLE": drop_table,
    "CHOMPCHOMP": drop_table,
}


def parse_line(line: str):
    engine = SQLEngine()
    output = engine.parse_sql(line)
    print(output)
