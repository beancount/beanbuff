#!/usr/bin/env python3
"""Create a mapping of transactions and order ids form the API.

The purpose is to add transaction ids to the data imported from the account
statement, and vice-versa, add order ids to the data imported to another
system,that doesn't have them.

The API is the only location which contains both the transaction ids and the
order ids.
"""
__author__ = 'Martin Blais <blais@furius.ca>'

from decimal import Decimal
from pprint import pformat
from pprint import pprint
from typing import Any, Dict, List, Optional, Set, Tuple
import argparse
import os
import collections
import datetime
import inspect
import logging
import re
import sys
import uuid
import pprint

import click
from dateutil import parser

import ameritrade
from ameritrade import utils

from beanbuff.data.etl import petl, Table, Record

# from beancount.core import flags
# from beancount.core import getters
# from beancount.core import inventory
# from beancount.core.amount import Amount
# from beancount.core.inventory import MatchResult
# from beancount.core.number import D
# from beancount.core.number import MISSING
# from beancount.core.number import ZERO
# from beancount.core.position import Cost
# from beancount.core.position import CostSpec
# from beancount.ops import summarize
# from beancount.parser import booking
# from beancount.parser.options import OPTIONS_DEFAULTS
from beancount import loader
from beancount.core import data
from beancount.parser import printer


def FetchMapping(config: ameritrade.Config,
                 start: datetime.date,
                 end: datetime.date) -> Table:
    """Fetch the mapping between transaction ids and other data from Ameritrade."""

    # Open a connection and figure out the main account.
    api = ameritrade.open(config)
    accountId = utils.GetMainAccount(api)

    # Fetch transactions.
    txns = api.GetTransactions(accountId=accountId, startDate=start, endDate=end)
    fields = ['transactionId', 'orderId', 'transactionDate', 'type', 'description']
    rows = [fields]
    for txn in txns:
        rows.append([txn.get(x, None) for x in fields])
    return (petl.wrap(rows)
            .convert('orderId', lambda v: utils.NormalizeOrderId(v) if v else None))


def GetLedgerTransactions(filename: str) -> List[data.Transaction]:
    """Get the list of transactions to exclude from the portfolio."""

    account_re = 'Assets:US:Ameritrade:Main'
    min_date = datetime.date(2021, 1, 1)
    order_link_re = 'order-(T.*)'
    filename = os.getenv('L')

    entries, _, options_map = loader.load_file(filename)
    for entry in data.filter_txns(entries):
        if entry.meta['filename'] != filename:
            continue
        if entry.date < min_date:
            continue
        if not any(re.match(account_re, posting.account)
               for posting in entry.postings):
            continue
        if any(re.match(order_link_re, link) for link in entry.links):
            continue

        printer.print_entry(entry)


def main():
    argparser = argparse.ArgumentParser()
    ameritrade.add_args(argparser)

    argparser.add_argument('-s', '--start', action='store',
                           help="Start date of the period to fetch.")

    argparser.add_argument('-e', '--end', action='store',
                           help="End date of the period to fetch.")

    argparser.add_argument('-l', '--ledger', action='store',
                           help=("Beancount ledger to remove already imported "
                                 "transactions (optional)."))

    args = argparser.parse_args()

    # Establish period of interest.
    today = datetime.date.today()
    start = parser.parse(args.start).date() if args.start else today.replace(month=1, day=1)
    end = parser.parse(args.end).date() if args.end else today
    table = FetchMapping(ameritrade.config_from_args(args), start, end)
    #print(table.lookallstr())

    if args.ledger:
        entries = GetLedgerTransactions(args.ledger)



if __name__ == '__main__':
    main()
