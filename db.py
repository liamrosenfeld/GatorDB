import os
import pickle
from typing import Any, Dict, List
from enum import Enum
import json

import numpy as np
from gdb_bplustree import BPlusTree

from query import Change, Condition, ConditionType
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
    def __init__(self, name: str, dbtype: DBType, path, order=50):
        self.path: str = path
        self.name: str = name

        self.tree: BPlusTree = BPlusTree(path=f"{path}/{name}.tree", max_degree=order)

    def insert(self, key, value):
        self.tree.insert(key, value)

    def delete(self, key):
        self.tree.delete(key)

    def values(self):
        return self.tree

    def save(self):
        self.tree.save()


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
            return np.array([])
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
    def __init__(self, name: str, col_info: ColumnInfo = None, path: str = ""):

        self.path = "/".join([path, name])
        self.name = name

        if col_info:
            self.col_info = col_info
        else:
            self.col_info = pickle.load(open(self._col_info_path(), "rb"))

        self.index_type = (
            ClusteredIndex if self.col_info.primary_key else NonclusteredIndex
        )
        self.index = self.index_type(name, dbtype=self.col_info.dbtype, path=self.path)

        if not os.path.isdir(self.path):
            os.mkdir(self.path)

    def insert(self, data, pk: int | str):
        self.index.insert(data, pk)

    def get(self, key):
        return self.index.get(key)

    def delete(self, key, value=None):
        if self.index_type == ClusteredIndex:
            self.index.delete(key)
        else:
            self.index.delete(key, value)

    def _col_info_path(self):
        return f"{self.path}/{self.name}.col"

    def save(self):
        pickle.dump(self.col_info, open(self._col_info_path(), "wb"))
        self.index.save()


class DBTable:
    def __init__(self, name: str = "Default Table", path: str = ""):
        self.path = "/".join([path, name])
        self.name = name
        self.cols: Dict[str, Column] = {}
        self.primary_key = None

        if os.path.isdir(self.path):
            if not os.path.isfile(self._cols_path()):
                raise FileNotFoundError(
                    "Corrupted GatorDB database: missing `cols` file"
                )
            cols = json.load(open(self._cols_path(), "r"))
            # os.listdir(self.path)
            for col in cols:
                self.cols[col] = Column(name=col, path=self.path)
                info = self.cols[col].col_info
                if info.primary_key:
                    self.primary_key = col
        else:
            os.mkdir(self.path)

    def _is_valid_shape(self, data: Dict[str, Any]) -> bool:
        return self.cols.keys() == data.keys()

    def _pk_col(self) -> Column:
        if not self.primary_key:
            raise ValueError("No primary key")
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

    def select(self, pks: np.ndarray) -> List[Dict]:
        """Returns dicts from pks"""
        if pks.size == 0:
            return []
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

    def delete(self, pks: np.ndarray) -> int:
        for pk in pks:
            data_dict = self._pk_col().get(pk)

            # get list of attributes
            attrs = list(data_dict.keys())[1:]  # don't include pk in attrs

            # delete nodes in nonclustered tree
            for attr in attrs:
                self.cols[attr].delete(data_dict[attr], pk)

            # delete pk in clustered tree
            self._pk_col().delete(pk)
        return pks.size

    def delete_all_rows(self):
        for col in self.cols:
            self.cols[col] = Column(
                name=col,
                col_info=self.cols[col].col_info,
                path=self.path,
            )

    def _cols_path(self):
        return self.path + "/cols"

    def save(self):
        for col in self.cols.values():
            col.save()
        json.dump(list(self.cols.keys()), open(self._cols_path(), "w"))


class DB(dict):
    def __init__(self, name: str, *args, **kwargs) -> "DB":
        super().__init__(*args, **kwargs)

        self.name = name

        if not os.path.isdir(name):
            os.mkdir(name)

        for table_name in os.listdir(name):
            self[table_name] = DBTable(name=table_name, path=name)
