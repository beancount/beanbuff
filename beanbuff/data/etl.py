"""PETL library import with our favorite global configuration parameters."""

from itertools import chain
from decimal import Decimal
from typing import List

import petl
petl.config.look_style = 'minimal'
petl.compat.numeric_types = petl.compat.numeric_types + (Decimal,)
petl.config.failonerror = True

Table = petl.Table
Record = petl.Record


def WrapRecords(records: List[Record]) -> Table:
    """Wrap up a list of records back to a table."""
    return petl.wrap([records[0].flds] + records)


def PrintGroups(table: Table, column: str):
    """Debug print groups of a table."""
    def pr(grouper):
        print(petl.wrap(grouper).lookallstr())
    agg = table.aggregate(column, pr)
    str(agg.lookallstr())
