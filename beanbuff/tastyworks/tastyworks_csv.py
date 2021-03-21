"""Tastyworks brokerage transaction download.

"""
import csv
import re
import itertools
import datetime
import collections
import typing
from typing import Any, Union, List, Optional
from decimal import Decimal

from ameritrade import options

from dateutil import parser
import petl
petl.config.look_style = 'minimal'

from beancount.core.amount import Amount
from beancount.core.position import CostSpec
from beancount.core.inventory import Inventory
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


OPTION_CONTRACT_SIZE = 100
Table = petl.Table
Record = petl.Record
debug = False
Config = Any


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'currency'            : 'Currency used for cash account',
        'asset_cash'          : 'Cash account',
        'asset_equity'        : 'Account for all positions, with {symbol} format',
        'asset_option'        : 'Account for stock positions',
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
        table = petl.fromcsv(file.name)
        ptable = PrepareTable(table)
        #print(ptable.lookallstr())

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
    assert row.Quantity == ZERO
    assert row['Average Price'] == ZERO

    if re.match('Wire Funds', row.Description):
        posting = txn.postings[0]
        txn.postings.append(
            posting._replace(account=config['third_party'],
                             units=-posting.units))

    elif re.match('Regulatory fee adjustment', row.Description):
        posting = txn.postings[0]
        txn.postings.append(
            posting._replace(account=config['fees'],
                             units=-posting.units))

    return [txn]


def DoTrade(row: Record, meta: dict, config: Config):
    txn = CreateTransaction(row, meta, config)
    optsym = ParseSymbol(row.Symbol)

    sign = 1 if row.Instruction == 'BUY' else -1
    units = Amount(sign * row.Quantity * row.Multiplier, optsym)

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
        raise NotImplementedError("No effect, not sure what to do.")

    txn.postings.insert(
        0, data.Posting(config['asset_option'], units, cost, price, None, None))

    return [txn]


_FUTSYM = "[A-Z0-9]+[FGHJKMNQUVXZ][0-9]"

def ParseFuturesOptionSymbol(symbol: str) -> str:
    # e.g., "./6JM1 JPUK1 210507P0.009" for futures option.
    match = re.match(fr"\.(/{_FUTSYM}) ({_FUTSYM}) (\d{{6}})([CP])([0-9.]+)", symbol)
    underlying = match.group(1)
    decade = datetime.date.today().year % 100 // 10
    underlying = underlying[:-1] + str(decade) + underlying[-1:]
    optname = match.group(2)
    side = match.group(4)
    strike = Decimal(match.group(5))

    # TODO(blais): Support options on futures... need to include the expiration date
    # AND the option name somehow.
    return f"{underlying}_{optname}{side}{strike}"


def ParseEquityOptionSymbol(symbol: str) -> str:
    # e.g. 'TLRY  210416C00075000' for equity option;
    underlying = symbol[0:6].rstrip()
    year = int(symbol[6:8])
    month = int(symbol[8:10])
    day = int(symbol[10:12])
    side = symbol[12]
    expiration = datetime.date(year, month, day)
    strike = Decimal(symbol[13:21]) / _PRICE_DIVISOR
    opt = options.Option(underlying, expiration, strike, side)
    return options.MakeOptionSymbol(opt)


_PRICE_DIVISOR = Decimal('1000')


def ParseSymbol(symbol: str) -> options.Option:
    if symbol.startswith("."):
        return ParseFuturesOptionSymbol(symbol)
    else:
        return ParseEquityOptionSymbol(symbol)




_HANDLERS = {
    'Money Movement': DoMoneyMovement,
    'Trade': DoTrade,
}


def Instruction(r: Record) -> Optional[str]:
    if r.Action.startswith('BUY'):
        return 'BUY'
    if r.Action.startswith('SELL'):
        return 'SELL'
    else:
        return None


def Effect(r: Record) -> Optional[str]:
    if r.Action.endswith('TO_OPEN'):
        return 'OPENING'
    elif r.Action.endswith('TO_CLOSE'):
        return 'CLOSING'
    else:
        return None


def ToDecimal(value: str):
    return Decimal(value.replace(',', '')) if value else ZERO


def PrepareTable(table: petl.Table) -> petl.Table:
    """Prepare the table for processing."""

    table = (table
             # Parse the date into datetime.
             .convert('Date', parser.parse)
             .convert('Expiration Date', lambda x: parser.parse(x).date())
             # Sort by date incremental.
             .sort('Date')
             # Split 'Action' field.
             .addfield('Instruction', Instruction)
             .addfield('Effect', Effect)
             # Convert fields to Decimal values.
             .convert(['Value',
                       'Average Price',
                       'Quantity',
                       'Commissions',
                       'Fees',
                       'Multiplier',
                       'Strike Price'], ToDecimal)
             )

    # Verify that the description field matches the provided field breakdowns.
    # TODO(blais): Do this.

    return table


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Tastyworks', config={
        'currency'            : 'USD',
        'asset_cash'          : 'Assets:US:Tastyworks:Cash',
        'asset_equity'        : 'Assets:US:Tastyworks:Stocks:{symbol}',
        'asset_option'        : 'Assets:US:Tastyworks:Options',
        'fees'                : 'Expenses:Financial:Fees',
        'commissions'         : 'Expenses:Financial:Commissions',
        'dividend'            : 'Income:US:Tastyworks:Stocks:{symbol}:Dividend',
        'pnl'                 : 'Income:US:Tastyworks:PnL',
        'third_party'         : 'Assets:US:Ameritrade:Main:Cash',
    })
    testing.main(importer)
