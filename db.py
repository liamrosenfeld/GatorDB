from typing import Any, Dict, List
from enum import Enum
import json

import numpy as np
from gdb_bplustree import BPlusTree

from query import Change, Condition, ConditionType
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
        dict_ = self.tree.get(pk)
        if not dict_:
            return None
        return json.loads(dict_.decode("utf-8"))


class NonclusteredIndex(Index):
    def __init__(self, name: str, dbtype: DBType, **kwargs):
        super().__init__(name, dbtype, **kwargs)

    def insert(self, data, pk):
        if self.tree.get(data) is None:
            self.tree.insert(data, np.array([pk], dtype=np.int32).tobytes())
        else:
            pointers_to_pk = self.get(data)
            self.tree[data] = np.concatenate(
                (pointers_to_pk, np.array([pk], dtype=np.int32)), dtype=np.int32
            ).tobytes()

    def get(self, data) -> np.ndarray:
        return np.frombuffer(self.tree.get(data), dtype=np.int32)


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

    def select(self, pks) -> List[Dict]:
        """Returns dicts from pks"""
        return [self._pk_col().index.get(pk=pk) for pk in pks]

    def filter(self, condition: Condition) -> List[int]:
        """Returns pks of items that match filter"""
        if condition.type == ConditionType.EQUALS:
            pks = self.cols[condition.col].get(condition.val)
        else:
            raise ValueError("Invalid condition code")
        return pks

    def insert(self, data: Dict[str, Any]):
        if not self._is_valid_shape(data):
            raise ValueError("Invalid shape")
        pk_value = data[self.primary_key]
        self._pk_col().insert(serialize_dict(data), pk_value)
        for col_name, col in self.cols.items():
            if col_name == self.primary_key:
                continue
            col.insert(data[col_name], pk_value)

    def update(self, pks: np.ndarray, changes: List[Change]):
        for change in changes:
            self.cols[change.col]
            # Set column to value

    def delete(self, pks: np.ndarray):
        for pk in pks:
            # delete pk in clustered tree
            # delete nodes in nonclustered tree
            pass
        pass

    def close(self):
        for col in self.cols.values():
            col.index.close()


class DB:
    def __init__(self, dir: str = "/tmp"):
        self.dest = dir
