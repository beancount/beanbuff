"""Read transactions and convert them to Beancount.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Set, Callable, Dict, List, Optional, Tuple, Iterator, Optional, Iterable
import types
import datetime
import hashlib
import logging
import sys
import functools
import pprint
import re
import os
import json

import click
from dateutil import parser
from more_itertools import first

from beancount.core.account import Account
from beancount.core.amount import Amount
from beancount.core.position import Cost
from beancount.core import data
from beancount.core import flags
from beancount.parser import printer
from beancount import loader

from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data import discovery
from johnny.base.etl import petl, Table, Record, WrapRecords
from beanbuff.data.transactions import GetTransactions
from beanbuff.data import consolidated


def ConvertTrade(rec: Record, config: Dict[str, str]) -> data.Transaction:
    """Convert a trade to a transaction."""

    meta = data.new_metadata('<{}>'.format(rec.account), 0)
    meta['datetime'] = str(rec.datetime)
    #meta['underlying'] = str(rec.underlying)

    tags = {rec.effect.lower()}
    links = {'order-{}'.format(rec.order_id),
             'buff-{}'.format(rec.transaction_id.lstrip('^'))}
    txn = data.Transaction(meta,
                           rec.datetime.date(), flags.FLAG_OKAY,
                           rec.rowtype, rec.description,
                           tags, links, [])

    sign = +1 if rec.instruction == 'BUY' else -1

    currency = 'USD'
    if rec.instype == 'Equity Option':
        units = Amount(sign * rec.quantity * rec.multiplier, rec.symbol)
        cost_number = rec.price
    else:
        units = Amount(sign * rec.quantity, rec.symbol)
        cost_number = rec.price * rec.multiplier

    if rec.effect == 'OPENING':
        cost, price = Cost(cost_number, currency, None, None), None
    else:
        cost, price = Cost(None, currency, None, None), Amount(cost_number, currency)
    txn.postings.append(
        data.Posting(config['trading'], units, cost, price, None, None))

    if rec.commissions:
        units = Amount(-rec.commissions, currency)
        txn.postings.append(
            data.Posting(config['commissions'], units, None, None, None, None))

    if rec.fees:
        units = Amount(-rec.fees, currency)
        txn.postings.append(
            data.Posting(config['fees'], units, None, None, None, None))

    total = (-sign * rec.quantity * rec.multiplier * rec.price +
             rec.commissions +
             rec.fees)
    units = Amount(total, currency)
    txn.postings.append(
        data.Posting(config['cash'], units, None, None, None, None))

    if rec.effect != 'OPENING':
        txn.postings.append(
            data.Posting(config['income'], None, None, None, None, None))

    return txn


def GetLedgerIds(
        filename: str,
        account_prefixes: str
) -> Tuple[Set[str], Set[str], Dict[str, datetime.date]]:
    """Get the list of transactions to exclude from the portfolio."""

    prefix_re = r'({}|Expenses|Income|Equity)'.format(
        '|'.join([re.escape(x) for x in account_prefixes]))
    has_prefix = re.compile(prefix_re).match

    order_ids = set()
    txn_ids = set()
    latest_date = {prefix: datetime.date(1970, 1, 1) for prefix in account_prefixes}
    entries, _, options_map = loader.load_file(filename)
    for entry in data.filter_txns(entries):
        # Accumulate order ids.
        for link in entry.links:
            match = re.match(r'order-.*', link)
            if match:
                order_ids.add(link)
            match = re.match(r'(buff|td)-.*', link)
            if match:
                txn_ids.add(link)

        # Get the latest date for transactions with all asset postings with the
        # prefix, regardless of ids. Note that this excludes transfer
        # transactions, which would have an asset account not prefixed.
        if all(has_prefix(p.account) for p in entry.postings):
            for prefix in account_prefixes:
                for posting in entry.postings:
                    if posting.account.startswith(prefix):
                        latest_date[prefix] = max(latest_date[prefix], entry.date)

    return order_ids, txn_ids, latest_date


def TagFilterPreviousEntry(entry: data.Transaction,
                           order_ids: Set[str],
                           txn_ids: Set[str],
                           latest_date: Dict[str, datetime.date]) -> data.Transaction:
    """Decorate or filter previous entries based on their presence in the ledger."""

    if entry.links & order_ids or entry.links & txn_ids:
        return None
        #entry.meta['imported'] = True

    # TODO(blais): Use 'latest_date'.

    return entry


@click.command()
@click.argument('config', type=click.Path(resolve_path=True, exists=True))
@click.argument('ledger', type=click.Path(resolve_path=True, exists=True))
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--html', type=click.Path(exists=False))
@click.option('--verbose', '-v', is_flag=True)
def main(config: str, ledger: str, fileordirs: List[str],
         html: str, verbose: bool):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the configuration.
    with open(config) as infile:
        config = json.load(infile)

    # Read the input files.
    transactions, filenames = GetTransactions(fileordirs)

    # Read previous state of the ledger.
    account_prefixes = set(c["prefix"] for c in config.values())
    order_ids, txn_ids, latest_date = GetLedgerIds(ledger, account_prefixes)

    # Add symbol.
    transactions = (transactions
                    .addfield('symbol', consolidated.SynthesizeSymbol))

    # Convert to transactions, unconditionally.
    undermap = collections.defaultdict(list)
    for rec in transactions.records():
        txn_config = config[rec.account]
        if rec.rowtype == 'Trade':
            entry = ConvertTrade(rec, txn_config)
        else:
            # TODO(blais):
            entry = None

        if entry:
            entry = TagFilterPreviousEntry(entry, order_ids, txn_ids, latest_date)
        if entry:
            undermap[rec.underlying].append(entry)

    # Group under the same underlying.
    outfile = sys.stdout
    pr = functools.partial(print, file=outfile)
    for underlying, entries in sorted(undermap.items()):
        pr('** {}'.format(underlying))

        printer.print_entries(data.sorted(entries), file=outfile)
        pr()
        pr()



if __name__ == '__main__':
    main()
