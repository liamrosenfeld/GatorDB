import unittest

from db import ColumnInfo, DBTable, DBType
from query import Change, Condition, ConditionType


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
        table.insert(
            {"pk": 2, "first_name": "John", "last_name": "Smith", "favorite_number": 22}
        )
        self.assertEqual(
            table.select_all(),
            [
                {
                    "pk": 1,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 15,
                },
                {
                    "pk": 2,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 22,
                },
            ],
        )
        self.assertEqual(
            list(
                table.filter(
                    condition=Condition(ConditionType.EQUALS, "first_name", "John")
                )
            ),
            [1, 2],
        )
        self.assertEqual(
            table.select(
                table.filter(
                    condition=Condition(ConditionType.EQUALS, "first_name", "John")
                )
            ),
            [
                {
                    "pk": 1,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 15,
                },
                {
                    "pk": 2,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 22,
                },
            ],
        )
        self.assertEqual(
            table.select(
                table.filter(
                    condition=Condition(ConditionType.EQUALS, "favorite_number", 22)
                )
            ),
            [
                {
                    "pk": 2,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 22,
                }
            ],
        )
        table.close()

    def test_delete_row(self):
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
        table.insert(
            {"pk": 2, "first_name": "John", "last_name": "Smith", "favorite_number": 22}
        )
        table.delete(
            table.filter(Condition(ConditionType.EQUALS, "favorite_number", 22))
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

        table.insert(
            {"pk": 2, "first_name": "John", "last_name": "Smith", "favorite_number": 22}
        )
        table.insert(
            {"pk": 3, "first_name": "John", "last_name": "Smith", "favorite_number": 33}
        )
        self.assertEqual(
            table.select_all(),
            [
                {
                    "pk": 1,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 15,
                },
                {
                    "pk": 2,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 22,
                },
                {
                    "pk": 3,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 33,
                },
            ],
        )

        table.delete(
            table.filter(Condition(ConditionType.EQUALS, "first_name", "John"))
        )
        self.assertEqual(table.select_all(), [])

    def test_update(self):
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
        table.insert(
            {"pk": 2, "first_name": "John", "last_name": "Smith", "favorite_number": 22}
        )
        table.update(
            table.filter(Condition(ConditionType.EQUALS, "favorite_number", 22)),
            [Change(col="favorite_number", val=89)],
        )
        self.assertEqual(
            table.select_all(),
            [
                {
                    "pk": 1,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 15,
                },
                {
                    "pk": 2,
                    "first_name": "John",
                    "last_name": "Smith",
                    "favorite_number": 89,
                },
            ],
        )

        table.update(
            table.filter(Condition(ConditionType.EQUALS, "first_name", "John")),
            [Change(col="first_name", val="Adam")],
        )
        self.assertEqual(
            table.select_all(),
            [
                {
                    "pk": 1,
                    "first_name": "Adam",
                    "last_name": "Smith",
                    "favorite_number": 15,
                },
                {
                    "pk": 2,
                    "first_name": "Adam",
                    "last_name": "Smith",
                    "favorite_number": 89,
                },
            ],
        )


# class TableLoadTests(unittest.TestCase):
#     def test_create_table(self):
#         table = DBTable(name="favorite_numbers_load", path="/tmp/")
#         table.add_column(
#             name="pk", col=ColumnInfo(dbtype=DBType.INTEGER, primary_key=True)
#         )
#         table.add_column(name="first_name", col=ColumnInfo(dbtype=DBType.STRING))
#         table.add_column(name="last_name", col=ColumnInfo(dbtype=DBType.STRING))
#         table.add_column(
#             name="favorite_number",
#             col=ColumnInfo(dbtype=DBType.INTEGER),
#         )
#         count = 100000
#         for i in range(count):
#             table.insert(
#                 {
#                     "pk": i,
#                     "first_name": "John " + str(i),
#                     "last_name": "Smith " + str(i),
#                     "favorite_number": i,
#                 }
#             )
#         self.assertEqual(len(table.select_all()), count)


if __name__ == "__main__":
    unittest.main()
