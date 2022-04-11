from typing import Any, Dict, List
from enum import Enum
import json

# from bplustree import BPlusTree, StrSerializer, IntSerializer

from gdb_bplustree import BPlusTree

from query import Equals
from utils import serialize_dict, serialize_str


class DBType(Enum):
    INTEGER = 0
    FLOAT = 1
    STRING = 2


class ColumnInfo:
    def __init__(self, dbtype: DBType = DBType.INTEGER, primary_key: bool = False):
        self.dbtype = dbtype
        self.primary_key = primary_key


class Index:
    def __init__(self, name: str, dbtype: DBType, path, order=50):
        # serializer = StrSerializer if dbtype == dbtype.STRING else IntSerializer

        self.path: str = path
        self.name: str = name
        self.tree: BPlusTree = BPlusTree(order=order)
        # self.tree: BPlusTree = BPlusTree(
        #     self._get_disk_location(), order=order, serializer=serializer()
        # )
        # print(self.tree)

    def _get_disk_location(self):
        return "-".join([self.path, self.name + ".db"])

    def insert(self, key, value):
        self.tree.insert(key, value)

    def values(self):
        return self.tree

    def close(self):
        pass
        # self.tree.close()


class ClusteredIndex(Index):
    def __init__(self, name: str, dbtype: DBType, **kwargs):
        super().__init__(name, dbtype, **kwargs)

    def insert(self, data: bytes, pk):
        self.tree.insert(pk, data)

    def get(self, pk) -> dict:
        return json.loads(self.tree.get(pk).decode("utf-8"))


class NonclusteredIndex(Index):
    def __init__(self, name: str, dbtype: DBType, **kwargs):
        super().__init__(name, dbtype, **kwargs)

    def insert(self, data, pk):
        if self.tree.get(data) is None:
            self.tree.insert(data, bytes([pk]))
        else:
            pointers_to_pk = self.get(data)
            self.tree[data] = bytes(pointers_to_pk + [pk])

    def get(self, data) -> list:
        return list(self.tree.get(data))


class Column:
    def __init__(
        self,
        name: str,
        col_info: ColumnInfo,
        path: str = "/tmp/",
    ):
        self.col_info = col_info

        index_type = ClusteredIndex if col_info.primary_key else NonclusteredIndex
        self.index = index_type(name, dbtype=col_info.dbtype, path=path)

    def insert(self, data, pk: int | str):
        self.index.insert(data, pk)

    def get(self, key):
        return self.index.get(key)


class DBTable:
    def __init__(self, name: str = "Default Table", path: str = "/tmp/"):
        self.path = "".join([path, name])
        self.name = name
        self.cols: Dict[str, Column] = {}
        self.primary_key = None

    def _is_valid_shape(self, data: Dict[str, Any]) -> bool:
        return self.cols.keys() == data.keys()

    def _pk_col(self) -> Column:
        return self.cols[self.primary_key]

    def set_primary_key(self, primary_key: str):
        if not (primary_key in self.cols):
            raise ValueError("Primary key column not in table")
        self.primary_key = primary_key

    def add_column(self, name: str, col: ColumnInfo):
        self.cols[name] = Column(name=name, col_info=col, path=self.path)
        if col.primary_key or not self.cols:
            if self.primary_key:
                raise ValueError("Primary key already designated")
            self.set_primary_key(name)

    def select_all(self) -> List[Dict]:
        return [
            json.loads(value.decode("utf-8"))
            for key, value in self._pk_col().index.values()
        ]

    def select_equals(self, equals: Equals) -> List[Dict]:
        pks = self.cols[equals.col].get(equals.val)
        return [self._pk_col().get(pk) for pk in pks]

    def insert(self, data: Dict[str, Any]):
        if not self._is_valid_shape(data):
            raise ValueError("Invalid shape")
        pk_value = data[self.primary_key]
        self._pk_col().insert(serialize_dict(data), pk_value)
        for col_name, col in self.cols.items():
            if col_name == self.primary_key:
                continue
            col.insert(data[col_name], pk_value)

    def close(self):
        for col in self.cols.values():
            col.index.close()


class DB:
    def __init__(self, dir: str = "/tmp"):
        self.dest = dir
