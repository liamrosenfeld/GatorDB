from typing import Any, List, NamedTuple


Equals = NamedTuple("Equals", (("col", str), ("val", Any)))
Query = NamedTuple("Query", (("Params", List[Equals]), ("Test", str)))
