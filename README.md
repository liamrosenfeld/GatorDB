# GatorDB

GatorDB is a primitive proof-of-concept implementation of an SQL database in Python using B+ trees. It supports interactive execution using primitive SQL syntax, reading a GatorDB database from a CSV file, and usage through the `DBTable` API for integration in other applications (e.g. as the backend of a CRUD application, though this is not tested).

## Install

Python 3.8+

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Interactive Mode

```sh
python3 gatordb.py --interactive --dbpath <path>

# Example
python3 gatordb.py --interactive --dbpath customer_orders
```

If you don't specify `dbpath`, the it will default to `database`.

#### Available types

- `INTEGER` or `INT`
- `TEXT` or `VARCHAR` (these are equivalent in GatorSQL and have no length bounds)
- `FLOAT`

#### Supported Operations

##### Meta Commands

These commands are not SQL statements. They are merely useful helper commands for running in interactive mode.

**LIST TABLES**

```
list_tables
tables
.tables
\dt
```

Any of these commands will print all the tables in the current database as a list.

**EXIT**

```
exit
quit
```

Exits interactive mode.

##### SQL Commands

**TABLE CREATION**

```sql
CREATE TABLE <table_name> (<pk_col_name> integer primary key, <col_name1> <col_type1>, ...)
```

The primary key must be an integer. The recommended name for this column is `pk` for brevity.

**CREATE**

```sql
INSERT INTO <table_name> VALUES (<col1> <val1>, <col2> <val2>, ...)
```

**READ**

```sql
SELECT <table_name>
SELECT * FROM <table_name>
SELECT * FROM <table_name> WHERE <col> = <val>
```

GatorDB does not support querying specific columns. Use `SELECT * FROM` or omit the `* FROM` clause entirely when reading from the table.

The `WHERE` clause only supports a single comparison using the equals operator.

**UPDATE**

```sql
UPDATE <table_name> SET <col> = <val> WHERE <col> = <val>
```

Update all values of a column for the rows matched by a given condition to the new desired value.

**DELETE**

```sql
DELETE FROM <table_name> WHERE <col> = <val>
```

Deletes all rows where the condition is matched.

**TRUNCATE**

```sql
TRUNCATE TABLE <table_name>
```

**DROP**

```sql
DROP TABLE <table_name>
```

### CSV Mode

GatorDB allows insertion into a table through CSV files. This allows batch inserts for large volumes of data without using the programmatic Python API.

```
python3 gatordb.py --csv <csv-path> --csv-table <table-name> --dbpath <path>

# Example
python3 gatordb.py --csv orders.csv --csv-table orders --dbpath commerce
```

The default command flags are as follows:

- `dbpath = default_database`
- `csv-table = default_table`

### Auxilliary tools

To generate a sample CSV file with 1,000 data points, run

```
python3 csv_gen.py
```

This will generate a file called `large_test.csv` which can then be used in CSV mode as documented above.
