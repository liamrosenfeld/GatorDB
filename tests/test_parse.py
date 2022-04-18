import unittest

from sqlengine import SQLEngine


class TableTests(unittest.TestCase):
    def test_parse(self):
        """Create a dictionary where the key is the SQL statement and the value is a standard dictionary that is the
        expected output. Iterate through these SQL statements to ensure that the SQLEngine class is parsing the SQL
        statements correctly and producing the expected dictionary when it returns."""

        statements = {
            "create table fruits(fruitId integer, firstName varchar, lastName varchar, favorite float primary key);": {
                "type": "CREATE TABLE",
                "table_name": "fruits",
                "attributes": {
                    "fruitId": "integer",
                    "firstName": "varchar",
                    "lastName": "varchar",
                    "favorite": "float",
                },
                "primary_key": "favorite",
            },
            "CREATE TABLE fruits(fruitId integer, firstName varchar, lastName varchar, favorite float primary key)": {
                "type": "CREATE TABLE",
                "table_name": "fruits",
                "attributes": {
                    "fruitId": "integer",
                    "firstName": "varchar",
                    "lastName": "varchar",
                    "favorite": "float",
                },
                "primary_key": "favorite",
            },
            "HATCH fruits(fruitId integer, firstName varchar, lastName varchar, favorite float primary key)": {
                "type": "CREATE TABLE",
                "table_name": "fruits",
                "attributes": {
                    "fruitId": "integer",
                    "firstName": "varchar",
                    "lastName": "varchar",
                    "favorite": "float",
                },
                "primary_key": "favorite",
            },
            "SELECT fruits": {
                "type": "SELECT",
                "table_name": "fruits",
                "conditions": {},
            },
            "swipe fruits": {
                "type": "SELECT",
                "table_name": "fruits",
                "conditions": {},
            },
            "select fruits where fruit_name = apple": {
                "type": "SELECT",
                "table_name": "fruits",
                "conditions": {"fruit_name": "apple"},
            },
            "swipe fruits where fruit_name = apple": {
                "type": "SELECT",
                "table_name": "fruits",
                "conditions": {"fruit_name": "apple"},
            },
            'insert into fruits values(1,2,3, "44", 5.5);': {
                "type": "INSERT INTO",
                "table_name": "fruits",
                "values": [1, 2, 3, "44", 5.5],
            },
            "truncate fruits": {
                "type": "TRUNCATE",
                "table_name": "fruits",
                "conditions": {},
            },
            "chomp fruits": {
                "type": "TRUNCATE",
                "table_name": "fruits",
                "conditions": {},
            },
            "truncate fruits where fruitName = 'apple'": {
                "type": "TRUNCATE",
                "table_name": "fruits",
                "conditions": {"fruitName": "apple"},
            },
            "drop table fruits": {"type": "DROP TABLE", "table_name": "fruits"},
            "SWAMP fruits": {"type": "DROP TABLE", "table_name": "fruits"},
        }

        engine = SQLEngine()
        for statement, expected_output in statements.items():
            actual_output = engine.parse_sql(statement)
            self.assertEqual(actual_output, expected_output)


if __name__ == "__main__":
    unittest.main()
