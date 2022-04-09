from typing import Any, Dict, List
from enum import Enum
import json

from bplustree import BPlusTree, StrSerializer, IntSerializer

from utils import serialize_dict


class DBType(Enum):
    INTEGER = 0
    FLOAT = 1
    STRING = 2


class ColumnInfo:
    def __init__(self, dbtype: DBType = DBType.INTEGER, primary_key: bool = False):
        self.dbtype = dbtype
        self.primary_key = primary_key


class Index:
    def __init__(self, name: str, dbtype: DBType, path: str = "/tmp/", order=50):
        serializer = StrSerializer if dbtype == dbtype.STRING else IntSerializer

        self.path: str = path
        self.name: str = name
        self.tree: BPlusTree = BPlusTree(
            self._get_disk_location(), order=order, serializer=serializer()
        )

    def _get_disk_location(self):
        return "".join([self.path, self.name, ".db"])

    def insert(self, key, value):
        self.tree.insert(key, value)


class ClusteredIndex(Index):
    def __init__(self, name: str, dbtype: DBType, **kwargs):
        super().__init__(name, dbtype, **kwargs)


class NonclusteredIndex(Index):
    def __init__(self, name: str, dbtype: DBType, **kwargs):
        super().__init__(name, dbtype, **kwargs)


class Column:
    def __init__(
        self,
        name: str,
        col_info: ColumnInfo,
        primary_key: bool = False,
        path: str = "/tmp/",
    ):
        self.col_info = col_info

        index_type = ClusteredIndex if primary_key else NonclusteredIndex
        self.index = index_type(name, dbtype=col_info.dbtype, path=path)

    def insert(self, data, pk: bytes):
        self.index.insert(data, pk)


class DBTable:
    def __init__(self, name: str = "Default Table", path: str = "/tmp/"):
        self.path = path
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
        self.cols[name] = Column(
            name=name, col_info=col, primary_key=col, path=self.path
        )
        if col.primary_key or not self.cols:
            if self.primary_key:
                raise ValueError("Primary key already designated")
            self.set_primary_key(name)

    def select_all(self) -> List[Dict]:
        return [
            json.loads(value.decode("utf-8"))
            for value in self._pk_col().index.tree.values()
        ]

    def select_column(self, col_name, col_val):
        pass

    def insert(self, data: Dict[str, Any]):
        if not self._is_valid_shape(data):
            raise ValueError("Invalid shape")
        pk_value = data[self.primary_key]
        self._pk_col().insert(pk_value, serialize_dict(data))
        for col_name, col in self.cols.items():
            if col_name == self.primary_key:
                continue
            col.insert(data[col_name], bytes(pk_value))

    def close(self):
        for col in self.cols.values():
            col.index.tree.close()


class DB:
    def __init__(self, dir: str = "/tmp"):
        self.dest = dir
