"""Parse transactions history files.

This produces a standardized transactions history log and a separate
non-transaction log. See bottom for public entry points.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Callable, List, Optional, Tuple, Iterator, Optional, Iterable
import types
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser
from more_itertools import first

from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data import discovery
from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.tastyworks import tastyworks_transactions
from beanbuff.ameritrade import thinkorswim_transactions


def GetTransactions(fileordirs: List[str]) -> Tuple[Table, List[str]]:
    """Find files and parse and concatenate contents."""

    matches = discovery.FindFiles(
        fileordirs, [
            tastyworks_transactions.MatchFile,
            thinkorswim_transactions.MatchFile
        ])

    filenames = []
    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        transactions, _ = parser(filename)
        if not transactions:
            continue
        filenames.append(filename)

        # Note: These need to be processed by file, separately.
        # TODO(blais): Process 'other' transactions.
        transactions = match.Match(transactions)
        transactions = chains.Group(transactions)
        tables.append(transactions)

    table = petl.cat(*tables) if tables else petl.empty()
    return table, filenames


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--html', type=click.Path(exists=False))
@click.option('--verbose', '-v', is_flag=True)
@click.option('--no-equity', is_flag=True)
def main(fileordirs: List[str], html: str, verbose: bool, no_equity: bool=True):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    transactions, filenames = GetTransactions(fileordirs)

    # Remove equity if desired.
    #
    # TODO(blais): Handle this by subtracting existing transactions from the
    # Ledger instead.
    if no_equity:
        transactions = (transactions
                        .select(lambda r: r.instype != 'Equity'))

    print(transactions.lookallstr())


if __name__ == '__main__':
    main()
