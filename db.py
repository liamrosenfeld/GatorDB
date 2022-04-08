from typing import Dict

from enum import Enum


class DBType(Enum):
    INTEGER = 0
    FLOAT = 1
    STRING = 2


class Column:
    def __init__(self, dbtype: DBType = DBType.INTEGER):
        self.dbtype = dbtype


class DBTable:
    def __init__(self, name: str = "Default Table"):
        self.name = name
        self.cols: Dict[str, Column] = {}
        self.primary_key = None

    def set_primary_key(self, primary_key: str):
        if not primary_key in self.cols:
            raise ValueError("Primary key column not in table")
        self.primary_key = primary_key
        self._index_clustered(col=primary_key)

    def _index_clustered(self, col: str):
        pass

    def _index_nonclustered(self, col: str):
        pass

    def add_column(self, name: str, col: Column, primary_key: bool = False):
        if primary_key or not self.cols:
            self.set_primary_key(name)
        self.cols[name] = col
