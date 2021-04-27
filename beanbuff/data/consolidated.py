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

import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.data import positions
from beanbuff.ameritrade import thinkorswim_positions


@click.command()
@click.argument('transactions_filename', type=click.Path(resolve_path=True, exists=True))
@click.argument('positions_filename', type=click.Path(resolve_path=True, exists=True))
def main(transactions_filename: str, positions_filename: str):
    """Main program."""

    if 0:
        trades_table = positions.FindAndReadInputFiles([transactions_filename], debug=0)
        print(trades_table.lookallstr())

    # if 1:
    #     positions_table = positions.FindAndReadInputFiles([transactions_filename], debug=0)



if __name__ == '__main__':
    main()
