from typing import Any, List, NamedTuple
from enum import Enum


class ConditionType(Enum):
    EQUALS = 0


Condition = NamedTuple(
    "Condition", (("type", ConditionType), ("col", str), ("val", Any))
)
Change = NamedTuple("Change", (("col", str), ("val", Any)))
Query = NamedTuple("Query", (("Params", List[Condition]), ("Test", str)))
