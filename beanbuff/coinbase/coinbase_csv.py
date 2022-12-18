"""Importer for Coinbase CSV "Account" report.

  (User) > Statements > Statements Tab > Generate > Account

Download the CSV file.
"""
__author__ = 'Martin Blais <blais@furius.ca>'

import itertools
import datetime
from typing import Optional
from os import path
import pprint
from typing import Iterable
from functools import partial

import petl
from petl import Record
from dateutil import parser

from beancount.core import data
from beancount.core import amount
from beancount.core import flags
from beancount.parser import printer

from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


class Importer(beangulp.Importer):

    def __init__(self, filing):
        self._account = filing

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        header = ("portfolio,type,time,amount,balance,amount/balance unit,transfer id,"
                  "trade id,order id")
        return (utils.is_mimetype(filepath, 'text/csv') and
                utils.search_file_regexp(filepath, header))

    def date(self, filepath: str) -> Optional[datetime.date]:
        max_date = max(
            petl.fromcsv(filepath)
            .convert('time', lambda v: parser.parse(v).replace(tzinfo=None).date())
            .values('time'))
        return max_date

    def filename(self, filepath: str) -> Optional[str]:
        return 'coinbase.{}'.format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        return extract(filepath, self._account)


def leaf_for(currency: str) -> data.Account:
    return 'Cash' if currency == 'USD' else currency


def extract(filepath: str, root_account: str) -> data.Entries:
    table = (petl.fromcsv(filepath)

             # Convert date/time fields.
             .convert('time', lambda v: parser.parse(v).replace(tzinfo=None))
             .addfield('date', lambda r: r.time.date())

             # Convert number fields.
             .convert('amount', utils.parse_amount)
             .convert('balance', utils.parse_amount)

             # Create a unique transaction id field.
             .addfield('transaction_id', lambda r: r['order id'] or r['transfer id'])
             .cutout('order id', 'transfer id', 'trade id')
             .cutout('portfolio')

             # Rename some field names.
             .rename({'amount/balance unit': 'currency'})
             )

    # Create transactions.
    aggfuncs = {
        'transaction': partial(create_transaction, root_account=root_account),
        'datetime': ('time', lambda times: min(times)),
    }
    txn_table = (table.aggregate('transaction_id', aggfuncs)
                 .sort('datetime')
                 .values('transaction'))

    # Create final balances.
    last_table = (table.groupselectlast('currency')
                  .cut('currency', 'date', 'balance'))
    meta = data.new_metadata(f"<{__file__}>".format, 0)

    last_day = max(last_table.values('date')) + datetime.timedelta(days=1)

    balances = [
        data.Balance(meta, last_day,
                     "{}:{}".format(root_account, leaf_for(last_row.currency)),
                     amount.Amount(last_row.balance, last_row.currency), None, None)
        for last_row in last_table.records()]

    return list(itertools.chain(txn_table, balances))


def create_transaction(group: Iterable[Record], root_account: str) -> data.Transaction:
    """Create a single transaction from a group with matching id."""
    rows = list(group)
    frow = rows[0]

    # Partition rows.
    match_rows, other_rows = [], []
    for row in rows:
        (match_rows if row.type == 'match' else other_rows).append(row)

    # Make up a narration
    types = {row.type for row in match_rows}
    types.discard('fee')
    types_str = ' '.join(types)
    currencies = {row.currency for row in match_rows}
    currencies_str = ','.join(sorted(currencies))
    narration = f"{types_str} of {currencies_str}"

    # Create a transaction.
    meta = data.new_metadata(f"<{__file__}>".format, 0)
    links = {r.transaction_id for r in rows}
    postings = []
    txn = data.Transaction(meta, frow.date, flags.FLAG_OKAY, None, narration,
                           set(), links, postings)

    # Partition two-by-two the match rows in order to calculate the rates.
    assert len(match_rows) % 2 == 0, match_rows
    for _, group in itertools.groupby(enumerate(match_rows), lambda iv: iv[0]//2):
        # Characterize position and cash rows.
        group = list(group)
        assert len(group) == 2, group
        for _, row in group:
            if row.currency == 'USD':
                cash_row = row
            else:
                pos_row = row
        del row

        # Create transaction legs.
        price = amount.Amount(abs(cash_row.amount / pos_row.amount), 'USD')
        units = amount.Amount(pos_row.amount, pos_row.currency)
        postings.append(
            data.Posting(f"{root_account}:{pos_row.currency}", units, None, price, None, None))

        units = amount.Amount(cash_row.amount, cash_row.currency)
        postings.append(
            data.Posting(f"{root_account}:Cash", units, None, None, None, None))

    # Create other legs: fees, transfers
    for row in other_rows:
        units = amount.Amount(row.amount, row.currency)

        # Fees.
        if row.type == 'fee':
            postings.append(
                data.Posting("Expenses:Financial:Fees", -units, None, None, None, None))
            postings.append(
                data.Posting(f"{root_account}:Cash", units, None, None, None, None))

        # Deposits.
        elif row.type == 'deposit':
            txn = txn._replace(narration=row.type)
            postings.append(
                data.Posting("Assets:Transfer", -units, None, None, None, None))
            postings.append(
                data.Posting(f"{root_account}:Cash", units, None, None, None, None))

        # Withdrawal.
        elif row.type == 'withdrawal':
            txn = txn._replace(narration=row.type)
            postings.append(
                data.Posting("Assets:Transfer", -units, None, None, None, None))
            postings.append(
                data.Posting(f"{root_account}:Cash", units, None, None, None, None))

        else:
            raise ValueError(f"Row type {type} is not supported.")

    return txn


if __name__ == '__main__':
    importer = Importer('Assets:US:CoinbasePro')
    testing.main(importer)
