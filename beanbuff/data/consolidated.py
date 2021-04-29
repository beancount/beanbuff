"""Consolidated adjusted positions table.

This joins a normalized transactions log with a normalized positions table to
provide a chain-based view of P/L adjusted to realized histories on the trade
chains.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Callable, List, Optional, Tuple
import types
import datetime
import hashlib
import logging
import pprint
import re
import os
import sys

import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.data import transactions
from beanbuff.data import positions


def SyntherizeSymbol(table: Table) -> Table:
    """Remove the symbol columns and replace them by a single symbol."""

underlying  expiration  expcode  putcall  strike


@click.command()
@click.argument('transactions_filename', type=click.Path(resolve_path=True, exists=True))
@click.argument('positions_filename', type=click.Path(resolve_path=True, exists=True))
def main(transactions_filename: str, positions_filename: str):
    """Main program."""

    if 1:
        trades_table = transactions.FindAndReadFiles([transactions_filename], debug=0)
        active_table = (trades_table
                        .selecteq('rowtype', 'Mark'))

    if 1:
        pos_table = positions.FindAndReadFiles([positions_filename], debug=0)

    if active_table.nrows() != pos_table.nrows():
        print(active_table.nrows(), file=sys.stderr)
        print(pos_table.nrows(), file=sys.stderr)
        raise ValueError("Tables above differ.")

    print(active_table.lookallstr())





    # if 1:
    #     reference = Decimal('417.52')
    #     pos_table, totals = positions.ConsolidatePositionStatement(table, reference)
    #     print(pos_table.lookallstr())



if __name__ == '__main__':
    main()
