import os
import shutil

from tabulate import tabulate

from db import DB, ColumnInfo, DBTable, DBType
from sqlengine import SQLEngine
from query import Condition, ConditionType, Change
from utils import bolden, print_bold, print_green, print_red

# SQL parsing engine
engine = SQLEngine()

# global tables


def initialize_db(name: str):
    global tables
    tables = DB(name=name)
    print(f"(Using database '{name}')")


def get_db_type(column_type: str):
    """
    Convert a data type string in SQL syntax into DBType enum
    :param column_type: SQL column type, e.g varchar
    :return: the DBType equivalent enum
    :raises: ValueError if the data type is not supported
    """
    column_type = column_type.lower()
    if column_type in ("integer", "int"):
        return DBType.INTEGER
    elif column_type in ("varchar", "text"):
        return DBType.STRING
    elif column_type == "float":
        return DBType.FLOAT
    raise ValueError("Unknown column type: " + column_type)


def convert_value_to_data_type(value, table, column_name):
    """
    Convert a string value into the correct Python data type for a particular column in a table
    :param value: value to convert, eg '50'
    :param table: table relation
    :param column_name: name of the column
    :return: the value converted to the correct data type
    :raises: ValueError if the column does not exist or an incompatible data type is supplied
    """
    try:
        column = table.cols[column_name]
        column_type = column.col_info.dbtype
        if column_type == DBType.INTEGER:
            return int(value)
        elif column_type == DBType.STRING:
            return str(value)
        if column_type == DBType.FLOAT:
            return float(value)
        else:
            raise ValueError(
                "Invalid column type for column %s in table %s"
                % (column_name, table.name)
            )
    except KeyError:
        raise ValueError(
            "The table %s does not have a column called %s" % (table.name, column_name)
        )


def create_table(table_name, attributes, primary_key):
    """
    Create a table
    :param table_name: name of the table
    :param attributes: table attributes (columns)
    :param primary_key: Primary key column
    :return: None
    :raises: ValueError if the table already exists
    """
    if table_name in tables:
        raise ValueError("Table %s already exists" % table_name)
    else:
        table = DBTable(name=table_name, path=tables.name)
        for column_name, column_type in attributes.items():
            is_primary_key = primary_key == column_name
            db_type = get_db_type(column_type)
            table.add_column(
                name=column_name,
                col=ColumnInfo(dbtype=db_type, primary_key=is_primary_key),
            )
        tables[table_name] = table
        print_green(
            "Successfully created table '%s' with %d columns"
            % (table_name, len(attributes))
        )


def select(table_name, where_colum, equals_value):
    """
    Select some information from the table
    :param table_name: name of the table
    :param where_colum: where column
    :param equals_value: equivalence value in the where column
    :return: None
    :raises: ValueError if the table does not exist
    """
    if table_name in tables:
        table = tables[table_name]
        if where_colum is None or equals_value is None:
            result = table.select_all()
        else:
            equals_value = convert_value_to_data_type(equals_value, table, where_colum)
            pks = table.filter(
                Condition(ConditionType.EQUALS, where_colum, equals_value)
            )
            result = table.select(pks)
        # print the results
        headers = list(table.cols.keys())
        table_data = []
        for i in result:
            table_data.append(list(i.values()))
        print_bold(tabulate(table_data, headers=headers))
    else:
        raise ValueError("Table %s does not exist" % table_name)


def insert_into(table_name, values):
    """
    Insert some rows into a table
    :param table_name: name of the table
    :param values: column values to insert
    :return: None
    :raises: ValueError if the table does not exist
    """
    if table_name in tables:
        table = tables[table_name]
        data = {}
        i = 0
        for column in list(table.cols.keys()):
            data[column] = values[i]
            i = i + 1
        table.insert(data)
        print_green("Successfully inserted 1 row into table %s" % table.name)
    else:
        raise ValueError("Table %s does not exist" % table_name)


def update(table_name, conditions, new_values):
    """
    Update table to change column values
    :param table_name: table name
    :param conditions: conditions matching
    :param new_values: new table values
    :return: None
    :raises: ValueError if the SQL statement is formatted badly
    """
    if table_name in tables:
        table = tables[table_name]
        condition_column_name = list(conditions.keys())[0]
        condition_column_value = convert_value_to_data_type(
            list(conditions.values())[0], table, condition_column_name
        )
        new_value_column_name = list(new_values.keys())[0]
        new_value_column_value = convert_value_to_data_type(
            list(new_values.values())[0], table, new_value_column_name
        )
        table.update(
            table.filter(
                Condition(
                    ConditionType.EQUALS, condition_column_name, condition_column_value
                )
            ),
            [Change(col=new_value_column_name, val=new_value_column_value)],
        )
        print_green("Successfully updated the table %s" % table_name)
    else:
        raise ValueError("Table %s does not exist" % table_name)


def delete(table_name, where_colum, equals_value):
    """
    Delete some data (rows) in the table
    :param table_name: name of the table
    :param where_colum: where column
    :param equals_value: equivalence value in the where column
    :return: None
    :raises: ValueError if the table does not exist
    """
    if table_name in tables:
        table = tables[table_name]
        if where_colum is None or equals_value is None:
            raise ValueError(
                "DELETE command missing condition. If deleting all conditions is desired, call TRUNCATE instead."
            )
        else:
            equals_value = convert_value_to_data_type(equals_value, table, where_colum)
            deleted_count = table.delete(
                table.filter(Condition(ConditionType.EQUALS, where_colum, equals_value))
            )
        if deleted_count > 0:
            print_green(
                "Successfully deleted %s rows from the table %s"
                % (deleted_count, table_name)
            )
        else:
            print_bold("No rows matched. Did not delete any rows.")
    else:
        raise ValueError("Table %s does not exist" % table_name)


def truncate(table_name, where_colum, equals_value):  # delete rows
    """
    Delete some data (rows) in the table
    :param table_name: name of the table
    :param where_colum: where column
    :param equals_value: equivalence value in the where column
    :return: None
    :raises: ValueError if the table does not exist
    """
    if table_name in tables:
        table = tables[table_name]
        if where_colum is None or equals_value is None:
            table.delete_all_rows()
        else:
            raise ValueError("Invalid TRUNCATE command")
        print_green("Successfully truncated table %s" % (table_name))
    else:
        raise ValueError("Table %s does not exist" % table_name)


def drop_table(table_name):
    """
    Delete a table
    :param table_name: name of the table
    :return: None
    :raises: ValueError if the table does not exist
    """
    if table_name in tables:
        table = tables[table_name]
        table.save()
        del tables[table_name]
        shutil.rmtree(os.path.join(tables.name, table_name))
        print_green("Successfully dropped the table %s" % table_name)
    else:
        raise ValueError("Table %s does not exist" % table_name)


def parse_line(line: str):
    """
    Parse an SQL line and print its executed statement
    :param line: SQL statement to parse
    :return: None
    """
    if line.lower() in ("list_tables", "tables", ".tables", "\\dt"):
        print_bold(", ".join(tables.keys()))
        print()
        return
    try:
        parsed = engine.parse_sql(line)
        if parsed["type"] == "CREATE TABLE":
            table_name = parsed["table_name"]
            attributes = parsed["attributes"]
            primary_key = parsed["primary_key"]
            create_table(table_name, attributes, primary_key)
            tables[table_name].save()
        elif parsed["type"] == "SELECT":
            table_name = parsed["table_name"]
            conditions = parsed["conditions"]
            where_colum = None if len(conditions) == 0 else list(conditions.keys())[0]
            equals_value = None if where_colum is None else conditions[where_colum]
            select(table_name, where_colum, equals_value)
        elif parsed["type"] == "INSERT INTO":
            table_name = parsed["table_name"]
            values = parsed["values"]
            insert_into(table_name, values)
            tables[table_name].save()
        elif parsed["type"] == "UPDATE":
            table_name = parsed["table_name"]
            conditions = parsed["conditions"]
            new_values = parsed["new_value"]
            update(table_name, conditions, new_values)
            tables[table_name].save()
        elif parsed["type"] == "DELETE":
            table_name = parsed["table_name"]
            conditions = parsed["conditions"]
            where_colum = None if len(conditions) == 0 else list(conditions.keys())[0]
            equals_value = None if where_colum is None else conditions[where_colum]
            delete(table_name, where_colum, equals_value)
            tables[table_name].save()
        elif parsed["type"] == "TRUNCATE":
            table_name = parsed["table_name"]
            conditions = parsed["conditions"]
            where_colum = None if len(conditions) == 0 else list(conditions.keys())[0]
            equals_value = None if where_colum is None else conditions[where_colum]
            truncate(table_name, where_colum, equals_value)
            tables[table_name].save()
        elif parsed["type"] == "DROP TABLE":
            table_name = parsed["table_name"]
            drop_table(table_name)
        else:
            raise ValueError("Unknown command")
    except Exception as e:
        print_red(str(e))
    print()
