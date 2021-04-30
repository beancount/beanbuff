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


def GetTransactions(fileordirs: List[str]) -> Table:
    """Find files and parse and concatenate contents."""

    matches = discovery.FindFiles(
        fileordirs,
        [tastyworks_transactions.MatchFile,
         thinkorswim_transactions.MatchFile])

    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        transactions, _ = parser(filename)

        # Note: These need to be processed by file, separately.
        # TODO(blais): Process 'other' transactions.
        transactions = match.Match(transactions)
        transactions = chains.Group(transactions)
        tables.append(transactions)

    return petl.cat(*tables)


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--html', type=click.Path(exists=False))
@click.option('--verbose', '-v', is_flag=True)
@click.option('--no-equity', is_flag=True)
def main(fileordirs: List[str], html: str, verbose: bool, no_equity: bool=True):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    transactions = GetTransactions(fileordirs)
    if not transactions:
        logging.fatal("No input files to read from the arguments.")
        return

    # Remove equity if desired.
    #
    # TODO(blais): Handle this by subtracting existing transactions from the
    # Ledger instead.
    if no_equity:
        transactions = (transactions
                        .select(lambda r: r.instype != 'Equity'))

    if 0:
        print(transactions.lookallstr()); raise SystemExit


# TODO(blais): Add EXPIRATIONS now!! I get incorrect output for TOS.

# TODO(blais): Remove orders from previous file.
# TODO(blais): Put generation time in file.

# TODO(blais): Split up chains between expirations?

# TODO(blais): Add the missing expirations!
# TODO(blais): Render P/L over all trades.
# TODO(blais): Make it possible to input the P50 on entry, somehow.
# TODO(blais): Fix futures positions.

# TODO(blais): Join with the positions table.
# TODO(blais): Calculate metrics (P/L per day).

# TODO(blais): Add average days in trade; scatter P/L vs. days in analysis.

# TODO(blais): Complete this, for the details page of a vertical.
# def RenderTrade(table: Table) -> str:
#     # Render a trade to something nicely readable.
#     #
#     # last_order_id = None
#     # cost = ZERO
#     # for row in rows:
#     #     if row.order_id != last_order_id:
#     #         print()
#     #         last_order_id = row.order_id
#     #     print("    {}".format(row.description))
#     # print()
#     # print()


if __name__ == '__main__':
    main()
