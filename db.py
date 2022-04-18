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
        self.tree: BPlusTree = BPlusTree(max_degree=order)
        # self.tree: BPlusTree = BPlusTree(
        #     self._get_disk_location(), order=order, serializer=serializer()
        # )
        # print(self.tree)

    def _get_disk_location(self):
        return "-".join([self.path, self.name + ".db"])

    def insert(self, key, value):
        self.tree.insert(key, value)

    def delete(self, key):
        self.tree.delete(key)

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
        pks = self.tree.get(data)
        if pks is None:
            return np.ndarray([])
        return np.frombuffer(pks, dtype=np.int32)

    def delete(self, data, pk):
        """
        If the data is duplicate, removes pointer of data to primary key.
        Otherwise, removes data itself.
        """
        pks = self.get(data)
        pks = np.delete(pks, np.where(pks == pk))
        if pks.size == 0:
            self.tree.delete(data)
        else:
            self.tree[data] = pks


class Column:
    def __init__(
        self,
        name: str,
        col_info: ColumnInfo,
        path: str = "/tmp/",
    ):
        self.col_info = col_info

        self.index_type = ClusteredIndex if col_info.primary_key else NonclusteredIndex
        self.index = self.index_type(name, dbtype=col_info.dbtype, path=path)

    def insert(self, data, pk: int | str):
        self.index.insert(data, pk)

    def get(self, key):
        return self.index.get(key)

    def delete(self, key, value=None):
        if self.index_type == ClusteredIndex:
            self.index.delete(key)
        else:
            self.index.delete(key, value)


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

    def filter(self, condition: Condition) -> np.ndarray:
        """Returns pks of items that match filter"""
        if condition.type == ConditionType.EQUALS:
            if not condition.col in self.cols:
                raise ValueError(f"Column '{condition.col}' does not exist")
            if condition.col == self.primary_key:
                return np.array([condition.val], dtype=np.int32)
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
        for pk in pks:
            data_dict = self._pk_col().get(pk)

            for change in changes:
                # Nonclustered Index: Remove and recreate pk pointer
                self.cols[change.col].delete(data_dict[change.col], pk)
                self.cols[change.col].insert(change.val, pk)

                # Clustered Index: Update dict
                data_dict[change.col] = change.val
                # Insert updates when key already exists
                self._pk_col().insert(serialize_dict(data_dict), pk)

    def delete(self, pks: np.ndarray):
        for pk in pks:
            data_dict = self._pk_col().get(pk)

            # get list of attributes
            attrs = list(data_dict.keys())[1:]  # don't include pk in attrs

            # delete nodes in nonclustered tree
            for attr in attrs:
                self.cols[attr].delete(data_dict[attr], pk)

            # delete pk in clustered tree
            self._pk_col().delete(pk)

    def close(self):
        for col in self.cols.values():
            col.index.close()


class DB:
    def __init__(self, dir: str = "/tmp"):
        self.dest = dir
