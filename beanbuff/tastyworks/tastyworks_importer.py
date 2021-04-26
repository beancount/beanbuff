"""Parse and normalize Tastyworks transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
"""
import csv
import re
import itertools
import datetime
import collections
import typing
from typing import Any, Dict, Union, List, Optional
from decimal import Decimal

from ameritrade import options

from dateutil import parser
import petl
petl.config.look_style = 'minimal'

from beancount.core.amount import Amount
from beancount.core.position import CostSpec
from beancount.core.inventory import Inventory
from beancount.core.account import Account
from beancount.core import data
from beancount.core import position
from beancount.core import inventory
from beancount.core import flags
from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core.number import MISSING
from beancount.utils import csv_utils
from beancount.utils.snoop import save

from beangulp import testing
from beangulp.importers.mixins import config
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier

from beanbuff.tastyworks import tastysyms
from beanbuff.tastyworks import tastyworks_transactions


Table = petl.Table
Record = petl.Record
debug = False
Config = Any


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'currency'            : 'Currency used for cash account',
        'asset_cash'          : 'Cash account',
        'asset_equity'        : 'Account for all positions, with {symbol} format',
        'asset_equity_option' : 'Account for all options',
        'asset_future'        : 'Account for all futures outright contracts',
        'asset_future_option' : 'Account for all futures options contracts',
        'fees'                : 'Fees',
        'commissions'         : 'Commissions',
        'dividend'            : 'Taxable dividend income, with {symbol} format',
        'pnl'                 : 'Capital Gains/Losses',
        'third_party'         : 'Other account for third-party transfers (wires)',
    }

    matchers = [
        ('mime', r'text/(plain|csv)'),
        ('content', 'Date,Type,Action,.*,Strike Price,Call or Put,Order #'),
    ]

    def extract(self, file):
        trade_table, other_table = tastyworks_transactions.GetTransactions(file.name)
        if 1:
            print()
            print()
            print(trade_table.lookallstr())
            print()
            print()

        entries = []
        for index, row in enumerate(ptable.records()):
            try:
                func = _HANDLERS[row.Type]
            except KeyError as exc:
                raise NotImplementedError(exc)
            else:
                meta = data.new_metadata(file.name, index * 100)
                func_entries = func(row, meta, self.config)
                if func_entries:
                    entries.extend(func_entries)
        return entries


def CreateTransaction(row: Record, meta: dict, config: Config) -> data.Transaction:
    meta['time'] = row.Date.time().strftime('%H:%M:%S')
    tags, links = set(), set()
    if row.Effect:
        tags.add(row.Effect.lower())
    if row['Order #']:
        links.add("order-{}".format(row['Order #']))
    txn = data.Transaction(
        meta, row.Date.date(), flags.FLAG_OKAY,
        None, row.Description, tags, links, [])

    cash = ZERO
    if row.Commissions:
        cash += row.Commissions
        txn.postings.append(
            data.Posting(config['commissions'],
                         Amount(-row.Commissions, config['currency']),
                         None, None, None, None))

    if row.Fees:
        cash += row.Fees
        txn.postings.append(
            data.Posting(config['fees'],
                         Amount(-row.Fees, config['currency']),
                         None, None, None, None))

    if row.Value:
        cash += row.Value

    if cash != ZERO:
        txn.postings.insert(0,
            data.Posting(config['asset_cash'],
                         Amount(cash, config['currency']),
                         None, None, None, None))

    return txn


def DoMoneyMovement(row: Record, meta: dict, config: Config):
    txn = CreateTransaction(row, meta, config)

    if re.match('Wire Funds', row.Description):
        posting = txn.postings[0]
        txn.postings.append(
            posting._replace(account=config['third_party'],
                             units=-posting.units))
        assert row.Quantity == ZERO, row
        assert row['Average Price'] == ZERO

    elif re.match('Regulatory fee adjustment', row.Description):
        posting = txn.postings[0]
        txn.postings.append(
            posting._replace(account=config['fees'],
                             units=-posting.units))
        assert row.Quantity == ZERO, row
        assert row['Average Price'] == ZERO

    else:
        # TODO(blais): Add transfers to/from margin on mark-to-market events or
        # not? How to ensure that the final P/L is accounted for properly if so?
        #
        #raise NotImplementedError("Row not handled: {}".format(row))
        txn = txn._replace(postings=[])

    return [txn]


def GetAccount(row: Record, config: Dict[str, Any]) -> Account:
    """Get the account corresponding to the row."""
    itype = row['Instrument Type']
    if itype == 'Equity':
        symbol = row['Underlying Symbol']
        return config['asset_equity'].format(symbol=symbol)
    elif itype == 'Equity Option':
        return config['asset_equity_option']
    elif itype == 'Future':
        symbol = row['Symbol']
        return config['asset_future'].format(symbol=symbol[1:])
    elif itype == 'Future Option':
        return config['asset_future_option']
    else:
        raise NotImplementedError("Unknown underlying type: {}".format(row))


def DoTrade(row: Record, meta: dict, config: Config):
    txn = CreateTransaction(row, meta, config)

    sign = 1 if row.Instruction == 'BUY' else -1
    units = Amount(sign * row.Quantity * row.Multiplier, row['BeanSym'])

    unit_price = row['Average Price'] / row.Multiplier
    unit_price = -sign * unit_price

    if row.Effect == 'OPENING':
        cost = CostSpec(unit_price, None, config['currency'], None, None, False)
        price = None
    elif row.Effect == 'CLOSING':
        cost = CostSpec(None, None, config['currency'], None, None, False)
        price = Amount(unit_price, config['currency'])
        txn.postings.append(
            data.Posting(config['pnl'], None, None, None, None, None))
    else:
        # Futures contracts have no opening/closing indicator. Treat them like
        # opening for now, and we'll fixup the closing bits by hand.
        cost = CostSpec(unit_price, None, config['currency'], None, None, False)
        price = None
        #raise NotImplementedError("No effect, not sure what to do: {}".format(row))

    account = GetAccount(row, config)
    txn.postings.insert(
        0, data.Posting(account, units, cost, price, None, None))

    return [txn]


_HANDLERS = {
    'Money Movement': DoMoneyMovement,
    'Trade': DoTrade,
}


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Tastyworks', config={
        'currency'            : 'USD',
        'asset_cash'          : 'Assets:US:Tastyworks:Main:Cash',
        'asset_equity'        : 'Assets:US:Tastyworks:Main:Equities',
        'asset_equity_option' : 'Assets:US:Tastyworks:Main:Equities',
        'asset_future'        : 'Assets:US:Tastyworks:Main:Futures',
        'asset_future_option' : 'Assets:US:Tastyworks:Main:Futures',
        'fees'                : 'Expenses:Financial:Fees',
        'commissions'         : 'Expenses:Financial:Commissions',
        'dividend'            : 'Income:US:Tastyworks:Stocks:{symbol}:Dividend',
        'pnl'                 : 'Income:US:Tastyworks:PnL',
        'third_party'         : 'Assets:US:Ameritrade:Main:Cash',
    })
    testing.main(importer)
