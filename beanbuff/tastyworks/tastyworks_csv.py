"""Tastyworks transactions download.

Click on "History" >> "Transactions" >> [period] >> [CSV]
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

from beanbuff.data import futures
from beanbuff.data import beansym


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
        table = petl.fromcsv(file.name)
        ptable = PrepareTable(table)
        if 0:
            print()
            print()
            print(ptable.lookallstr())
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


def ParseEquityOptionSymbol(symbol: str) -> str:
    # e.g. 'TLRY  210416C00075000' for equity option;
    return beansym.Instrument(
        underlying=symbol[0:6].rstrip(),
        expiration=datetime.date(int(symbol[6:8]), int(symbol[8:10]), int(symbol[10:12])),
        side=symbol[12],
        strike=Decimal(symbol[13:21]) / _PRICE_DIVISOR,
        multiplier=OPTION_CONTRACT_SIZE)


FUTSYM = "([A-Z0-9]+)([FGHJKMNQUVXZ])([0-9])"


def ParseFuturesSymbol(symbol: str) -> str:
    match = re.match(f"/{FUTSYM}", symbol)
    assert match
    underlying, fmonth, fyear = match.groups()
    underlying = f"/{underlying}"
    decade = datetime.date.today().year % 100 // 10
    multiplier = futures.MULTIPLIERS.get(underlying, 1)
    return beansym.Instrument(
        underlying=underlying,
        calendar=f"{fmonth}{decade}{fyear}",
        multiplier=multiplier)


def ParseFuturesOptionSymbol(symbol: str) -> str:
    # e.g., "./6JM1 JPUK1 210507P0.009" for futures option.
    match = re.match(fr"\./{FUTSYM} +{FUTSYM} +(\d{{6}})([CP])([0-9.]+)", symbol)

    underlying, fmonth, fyear = match.group(1,2,3)
    decade = datetime.date.today().year % 100 // 10
    calendar = f"{fmonth}{decade}{fyear}"

    optcontract, optfmonth, optfyear = match.group(4,5,6)
    optdecade = datetime.date.today().year % 100 // 10
    optcalendar = f"{optfmonth}{optdecade}{optfyear}"

    expistr = match.group(7)
    expiration = datetime.date(int(expistr[0:2]), int(expistr[2:4]), int(expistr[4:6]))
    side = match.group(8)
    strike = Decimal(match.group(9))

    return beansym.Instrument(
        underlying=f"/{underlying}",
        calendar=calendar,
        optcontract=optcontract,
        optcalendar=optcalendar,
        expiration=expiration,
        side=side,
        strike=strike,
        multiplier=1)  ## TODO(blais):


_PRICE_DIVISOR = Decimal('1000')


def ParseSymbol(symbol: str, itype: Optional[str]) -> options.Option:
    if not symbol:
        return None
    # Futures options always start with a period.
    inst = None
    if itype == 'Future Option' or itype is None and symbol.startswith("./"):
        inst = ParseFuturesOptionSymbol(symbol)
    # Futures always start with a slash.
    elif itype == 'Future' or itype is None and symbol.startswith("/"):
        inst = ParseFuturesSymbol(symbol)
    # Then we have options, with a space.
    elif itype == 'Equity Option' or itype is None and ' ' in symbol:
        inst = ParseEquityOptionSymbol(symbol)
    # And finally, just equities.
    elif itype == 'Equity' or itype is not None:
        inst = beansym.Instrument(underlying=symbol)
    else:
        raise ValueError(f"Unknown instrument type: {itype}")
    return inst


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


def FixMultiplier(_: str, rec: Record) -> int:
    multiplier = int(rec.Multiplier) if rec.Multiplier else 0
    itype = rec['Instrument Type']
    if not itype:
        pass
    elif itype == 'Future Option':
        assert multiplier == 1
        multiplier = futures.MULTIPLIERS[rec._Instrument.underlying]
    elif itype == 'Future':
        assert multiplier == 0
        multiplier = futures.MULTIPLIERS[rec._Instrument.underlying]
    elif itype == 'Equity Option':
        assert multiplier == 100
    elif itype == 'Equity':
        assert multiplier == 1
    else:
        raise ValueError(f"Unknown instrument type: {itype}")
    return multiplier


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
                       'Strike Price'], ToDecimal)

             # Create a normalized symbol.
             .addfield('_Instrument', lambda r: ParseSymbol(
                 r['Symbol'], r['Instrument Type']))
             .addfield('BeanSym', lambda r: (
                 beansym.ToString(r._Instrument) if r._Instrument else ''))

             # Set the multiplier for futures contracts.
             .convert('Multiplier', FixMultiplier, pass_row=True)
             .cutout('_Instrument')

             # Check out the contract value.
             #.addfield('ContractValue', lambda r: r.Multiplier * r['Strike Price'])
             )

    # Verify that the description field matches the provided field breakdowns.
    # TODO(blais): Do this.

    return table


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
