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

from more_itertools import first
import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.data import transactions as transactions_mod
from beanbuff.data import positions as positions_mod
from beanbuff.data import beansym


def SynthesizeSymbol(r: Record) -> str:
    """Remove the symbol columns and replace them by a single symbol."""
    return str(beansym.FromColumns(r.underlying,
                                   r.expiration,
                                   r.expcode,
                                   r.putcall,
                                   r.strike,
                                   r.multiplier))


def DebugPrint(tabledict):
    for name, table in tabledict.items():
        with open("/tmp/{}.txt".format(name), "w") as ofile:
            print(table.sort(), file=ofile)


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--html', type=click.Path(exists=False))
def main(fileordirs: str, html: str):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    if 1:
        transactions = transactions_mod.GetTransactions(fileordirs)
        if not transactions:
            logging.fatal("No input files to read from the arguments.")

    if 1:
        positions = positions_mod.GetPositions(fileordirs)
        if not positions:
            logging.fatal("No input files to read from the arguments.")

    # # TODO(blais): Do away with this eventually.
    # transactions = (transactions
    #                 .select(lambda r: r.instype != 'Equity'))

    # Keep only the open options positions in the transactions log.
    transactions = (transactions
                    .addfield('symbol', SynthesizeSymbol))

    # TODO(blais): Handle multiple accounts in the positions file.
    # DebugPrint(transactions, positions); return

    # Add column to match only mark rows to position rows.
    positions = (positions
                 .addfield('rowtype', 'Mark'))

    # Join positions to transactions.
    augmented = petl.outerjoin(transactions, positions,
                               key=['account', 'symbol', 'rowtype'], rprefix='p_')
    if transactions.nrows() != augmented.nrows():
        DebugPrint({'txn': transactions, 'pos': positions, 'aug': augmented})
        raise ValueError("Tables differ. See debug prints in /tmp.")

    # Convert to chains.
    chains = transactions_mod.TransactionsToChains(augmented)
    active_chains = (chains
                     .selecteq('active', True))

    # Clean up the chains and add targets.
    final_chains = transactions_mod.FormatActiveChains(active_chains)
    if html:
        final_chains.tohtml(html)
    print(final_chains.sort('tgtinit%').lookallstr())


# TODO(blais): Add a separate table to match, which provides an association of
# - Trade name/code
# - Comment for entry
# - Initial POP


if __name__ == '__main__':
    main()
