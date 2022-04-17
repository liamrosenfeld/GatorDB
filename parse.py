from db import ColumnInfo, DBTable, DBType
from sqlengine import SQLEngine
from query import Condition, ConditionType

# SQL parsing engine
engine = SQLEngine()

# Holds all the tables
tables = {}


def get_db_type(column_type):
    """
    Convert a data type string in SQL syntax into DBType enum
    :param column_type: SQL column type, e.g varchar
    :return: the DBType equivalent enum
    :raises: ValueError if the data type is not supported
    """
    if column_type == 'integer':
        return DBType.INTEGER
    elif column_type == 'varchar':
        return DBType.STRING
    elif column_type == 'float':
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
            raise ValueError("Invalid column type for column %s in table %s" % (column_name, table.name))
    except KeyError:
        raise ValueError("The table %s does not have a column called %s" % (table.name, column_name))


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
        table = DBTable(name=table_name, path="/tmp/s")
        for column_name, column_type in attributes.items():
            is_primary_key = primary_key == column_name
            db_type = get_db_type(column_type)
            table.add_column(name=column_name, col=ColumnInfo(dbtype=db_type, primary_key=is_primary_key))
        tables[table_name] = table
        print("Successfully created table '%s' with %d columns" % (table_name, len(attributes)))


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
            pks = table.filter(Condition(ConditionType.EQUALS,where_colum, equals_value))
            result = table.select(pks)
        print(result)
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
        print("Successfully inserted 1 row into table %s" % table.name)
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
            deleted_count = table.delete_all_rows()
        else:
            equals_value = convert_value_to_data_type(equals_value, table, where_colum)
            deleted_count = table.delete_equals(equals=Condition(ConditionType.EQUALS, where_colum, equals_value))
        print("Successfully deleted %s rows from the table %s" % (deleted_count, table_name))
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
        table.close()
        del tables[table_name]
        print("Successfully dropped the table %s" % table_name)
    else:
        raise ValueError("Table %s does not exist" % table_name)


def parse_line(line: str):
    """
    Parse an SQL line and print its executed statement
    :param line: SQL statement to parse
    :return: None
    """
    try:
        parsed = engine.parse_sql(line)
        if parsed['type'] == 'CREATE TABLE':
            table_name = parsed['table_name']
            attributes = parsed['attributes']
            primary_key = parsed['primary_key']
            return create_table(table_name, attributes, primary_key)
        elif parsed['type'] == 'SELECT':
            table_name = parsed['table_name']
            conditions = parsed['conditions']
            where_colum = None if len(conditions) == 0 else list(conditions.keys())[0]
            equals_value = None if where_colum is None else conditions[where_colum]
            return select(table_name, where_colum, equals_value)
        elif parsed['type'] == 'INSERT INTO':
            table_name = parsed['table_name']
            values = parsed['values']
            return insert_into(table_name, values)
        elif parsed['type'] == 'TRUNCATE':
            table_name = parsed['table_name']
            conditions = parsed['conditions']
            where_colum = None if len(conditions) == 0 else list(conditions.keys())[0]
            equals_value = None if where_colum is None else conditions[where_colum]
            return truncate(table_name, where_colum, equals_value)
        elif parsed['type'] == 'DROP TABLE':
            table_name = parsed['table_name']
            return drop_table(table_name)
        else:
            raise ValueError("Unknown command")
    except Exception as e:
        print(e)