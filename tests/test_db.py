import unittest

from db import ColumnInfo, DBTable, DBType


class TableTests(unittest.TestCase):
    def test_create_table(self):
        table = DBTable(name="favorite_numbers", path="/tmp/")
        table.add_column(
            name="pk", col=ColumnInfo(dbtype=DBType.INTEGER, primary_key=True)
        )
        table.add_column(name="first_name", col=ColumnInfo(dbtype=DBType.STRING))
        table.add_column(name="last_name", col=ColumnInfo(dbtype=DBType.STRING))
        table.add_column(
            name="favorite_number",
            col=ColumnInfo(dbtype=DBType.INTEGER),
        )
        table.insert(
            {"pk": 1, "first_name": "John", "last_name": "Smith", "favorite_number": 15}
        )
        self.assertEqual(
            table.select_all(),
            [
                {
                    "pk": 1,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 15,
                }
            ],
        )
        table.close()


if __name__ == "__main__":
    unittest.main()
