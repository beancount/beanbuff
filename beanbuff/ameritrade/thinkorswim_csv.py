"""Think-or-Swim "Account Statement" CSV detail importer.

Instructions:
- Start TOS
- Go to the "Monitor" tab
- Select the "Account Statement" page
- Select the desired time period
- Right on the rightmost hamburger menu and select "Export to File..."

"""
import csv
import re
import itertools
import datetime
import collections
import typing
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from decimal import Decimal
from functools import partial
from itertools import chain

from dateutil import parser
import petl
petl.config.look_style = 'minimal'
petl.config.failonerror = True

from beancount.core.number import ZERO
from beancount.core.number import ONE
from beancount.core.amount import Amount
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

from beanbuff.data.rowtxns import Txn, TxnType, Instruction, Effect
from beanbuff.data.futures import MULTIPLIERS


OPTION_CONTRACT_SIZE = Decimal(100)
Table = petl.Table
Record = petl.Record
debug = False
Config = Any


# Symbol name changes sometimes occur out of sync in the TOS platform. You may
# find the old symbol name in the trading history and the new one in the cash
# statement.
SYMBOL_NAME_CHANGES = {
    # https://investorplace.com/2021/03/chpt-stock-12-things-to-know-as-chargepoint-trading-spac-merger-sbe-stock/
    'CHPT': 'SBE',
}


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'currency'            : 'Currency used for cash account',
        'asset_cash'          : 'Cash account',
        'asset_money_market'  : 'Money market account associated with this account',
        'asset_forex'         : 'Retail foreign exchange trading account',
        'futures_contracts'   : 'Root account holding contracts',
        'futures_margin'      : 'Margin used, in dollars',
        'futures_cash'        : 'Cash account for futures only',
        'futures_pnl'         : 'Profit/loss on futures contracts',
        'futures_miscfees'    : 'Miscellanious fees',
        'futures_commissions' : 'Commissions',
        'asset_position'      : 'Account for all positions, with {symbol} format',
        'option_position'     : 'Account for options positions, with {symbol} format',
        'fees'                : 'Fees',
        'commission'          : 'Commissions',
        'interest'            : 'Interest income',
        'dividend_nontax'     : 'Non-taxable dividend income, with {symbol} format',
        'dividend'            : 'Taxable dividend income, with {symbol} format',
        'adjustment'          : 'Free / unknown / miscellaneous adjustment account',
        'pnl'                 : 'Capital Gains/Losses',
        'transfer'            : 'Other account for inter-bank transfers',
        'third_party'         : 'Other account for third-party transfers (wires)',
        'opening'             : 'Opening balances account, used to make transfer when you opt-in',
    }

    matchers = [
        ('mime', r'text/(plain|csv)')
    ]

    def extract(self, file):
        """Import a CSV file from Think-or-Swim."""
        tables = PrepareTables(file.name)

        # Pull out the trading log which contains trade information over all the
        # instrument but not any of the fees.
        tradehist = (tables['Account Trade History']
                         .addfield('quantity', lambda r: abs(r.qty)))

        # Split up the "Cash Balance" table and process non-trade entries.
        cashbal = tables['Cash Balance']
        equities_trade, cashbal_nontrade = SplitCashBalance(cashbal, tradehist)
        cashbal_entries = ProcessNonTradeCash(cashbal_nontrade)

        # Split up the "Futures Statements" table and process non-trade entries.
        futures = tables['Futures Statements']
        futures_trade, futures_nontrade = SplitFuturesStatements(futures, tradehist)
        futures_entries = ProcessNonTradeFutures(cashbal_nontrade)

        cash_trade = (petl.cat(equities_trade, futures_trade)
                      .sort('datetime'))

        # Match up the equities and futures statements entries to the trade
        # history and ensure a perfect match, returning groups of (date-time,
        # cash-rows, trade-rows), properly matched.
        equities_groups, futures_groups = ProcessTradeHistory(equities_trade, futures_trade, tradehist)

        if 0:
            # Debug print.
            for group in futures_groups:
                PrintGroup(group)

        # Convert matched groups of rows to trnasctions.
        equities_txns = ConvertGroupsToTransactions(equities_groups, False)
        futures_txns = ConvertGroupsToTransactions(futures_groups, True)

        # TODO(blais): Add type (FUTURES, ETF, CALL, PUT, etc.)
        # TODO(blais): Add finalized option symbol.

        if 1:
            # Debug print.
            txns = petl.cat(
                petl.wrap(chain([Txn._fields], equities_txns)),
                petl.wrap(chain([Txn._fields], futures_txns)))
            table = (petl.wrap(txns)
                     .addfield('size', lambda r: r.multiplier * r.price * r.quantity))
                     #.select('size', lambda v: v > 10000))
            print(table.lookallstr())

        # new_entries = []
        # if entries:
        #     new_entries.extend(entries)
        # return new_entries
        return []


def SplitCashBalance(statement: Table, tradehist: Table) -> Tuple[Table, Table]:
    """Split the cash statement between simple cash effects vs. trades.
    Trades includes expirations and dividend events."""

    # Strategy has been inferred from the preparation and can be used to
    # distinguish trading and non-trading rows.
    nontrade = statement.select(lambda r: not r.strategy)
    trade = statement.select(lambda r: bool(r.strategy))

    # Check that the non-trade cash statement transactions have no overlap
    # whatsoever with the trades on.
    keyed_statement = nontrade.aggregate('datetime', list)
    keyed_trades = tradehist.aggregate('exec_time', list)
    joined = petl.join(keyed_statement, keyed_trades,
                       lkey='datetime', rkey='exec_time',
                       lprefix='cash', rprefix='trade')
    if joined.nrows() != 0:
        raise ValueError("Statement table contains trade data: {}".format(joined))

    return trade, nontrade

def SplitFuturesStatements(futures: Table, tradehist: Table) -> Tuple[Table, Table]:
    """Split the cash statement between simple cash effects vs. trades.
    Trades includes expirations and dividend events."""

    # Splitting up the futures statement is trivial because the "Ref" columns is
    # present and consistently all trading data has a ref but not non-trading
    # data.
    nontrade = futures.select(lambda r: not r.ref)
    trade = futures.select(lambda r: bool(r.ref))

    # Check that the non-trade cash statement transactions have no overlap
    # whatsoever with the trades on.
    keyed_statement = nontrade.aggregate('datetime', list)
    keyed_trades = tradehist.aggregate('exec_time', list)
    joined = petl.join(keyed_statement, keyed_trades,
                       lkey='datetime', rkey='exec_time',
                       lprefix='cash', rprefix='trade')
    if joined.nrows() != 0:
        raise ValueError("Statement table contains trade data: {}".format(joined))

    return trade, nontrade


def ProcessNonTradeCash(nontrade: Table) -> data.Entries:
    """Produce the non-trade 'Cash Balance' entries."""
    # TODO(blais):
    return []

def ProcessNonTradeFutures(cash_nontrade: Table) -> data.Entries:
    """Produce the non-trade 'Futures Statements' entries."""
    # TODO(blais):
    return []


def ReconcilePairsOrderIds(table: Table, threshold: int) -> Table:
    """On a pairs trade, the time issued will be identical, but we will find two
    distinct symbols and order ids (one that is 1 or 2 integer apart). We reduce
    the ids to the smallest one by looking at increments below some threshold
    and squashing the later ones. This way we can link together pairs trades or
    blast-alls (probably).
    """
    def AdjustedOrder(head_id, rec: Record) -> int:
        if head_id[0] is None:
            head_id[0] = rec.order_id
            return rec.order_id
        diff = rec.order_id - head_id[0]
        if diff == 0:
            return rec.order_id
        if diff < threshold:
            return head_id[0]
        head_id[0] = rec.order_id
        return rec.order_id
    table = (
        table
        .sort('order_id')
        .addfield('adj_order_id', partial(AdjustedOrder, [None]))
        .addfield('order_diff',
                  lambda r: ((r.order_id - r.adj_order_id)
                             if (r.order_id != r.adj_order_id)
                             else '')))

    if 0:
        # Debug print.
        for order_id, group in table.aggregate('adj_order_id', list).records():
            if len(set(rec.order_id for rec in group)) > 1:
                print(petl.wrap(chain([table.header()], group)).lookallstr())

    return table


def ProcessTradeHistory(equities_cash: Table,
                        futures_cash: Table,
                        trade_hist: Table) -> Tuple[List[Any], List[Any]]:
    """Join the trade history table with the equities table.

    Note that the equities table does not contian the ref ids, so they we have
    to use the symbol as the second key to disambiguate further from the time of
    execution. (If TD allowed exporting the ref from the Cash Statement we would
    just use that, that would resolve the problem. There is a bug in the
    software, it doesn't get exported.)
    """

    trade_hist = ReconcilePairsOrderIds(trade_hist, 5)

    # We want to pair up the trades from the equities and futures statements
    # with the trades from the trade history table. We will aggregate the trade
    # history table by a unique key (using the time, seems to be pretty good)
    # and decimate it by matching rows from the cash tables. Then we verify that
    # the trade history has been fully accounted for by checking that it's empty.
    trade_hist_map = trade_hist.recordlookup('exec_time')

    # Process the equities cash table.
    def MatchTradingRows(cash_table: Table):
        order_groups = []
        mapping = cash_table.recordlookup('datetime')
        for dtime, cash_rows in mapping.items():
            # If the transaction is not a trade, ignore it.
            # Dividends, expirations and others will have to be processed elsewhere.
            if not any(crow.type == 'TRD' for crow in cash_rows):
                # Process dividends and expirations.
                continue

            # Pull up the rows corresponding to this cash statement and remove
            # them from the trade history.
            try:
                trade_rows = trade_hist_map.pop(dtime)
            except KeyError:
                raise KeyError("Trade history for cash row '{}' not found".format(crow))

            order_groups.append((dtime, cash_rows, trade_rows))

        return order_groups

    # Fetch the trade history rows for equities.
    equities_groups = MatchTradingRows(equities_cash)
    # Fetch the trade history rows for futures.
    futures_groups = MatchTradingRows(futures_cash)

    # Assert that the trade history table has been fully accounted for.
    if trade_hist_map:
        raise ValueError("Some trades from the trade history not covered by cash: "
                         "{}".format(trade_hist_map))

    return equities_groups, futures_groups


Group = Tuple[datetime.date, List[Record], List[Record]]


def PrintGroup(group: Group):
    dtime, cash_rows, trade_rows = group
    print("-" * 200)
    print(dtime)
    ctable = petl.wrap(chain([cash_rows[0].flds], cash_rows))
    print(ctable.lookallstr())
    ttable = petl.wrap(chain([trade_rows[0].flds], trade_rows))
    print(ttable.lookallstr())


def FindMultiplier(string: str) -> Decimal:
    """Find a multiplier spec in the given description string."""
    match = re.search(r"\b1/(\d+)\b", string)
    if not match:
        match = re.search(r"(?:\s|^)(/[A-Z0-9]*?)[FGHJKMNQUVXZ]2[0-9]\b", string)
        if not match:
            raise ValueError("No symbol to find multiplier: '{}'".format(string))
        symbol = match.group(1)
        try:
            multiplier = MULTIPLIERS[symbol]
        except KeyError:
            raise ValueError("No multiplier for symbol: '{}'".format(symbol))
        return Decimal(multiplier)
    return Decimal(match.group(1))


def ConvertGroupsToTransactions(groups: List[Group],
                                is_futures: bool,
                                quote_currency: str = 'USD') -> List[Txn]:
    """Convert groups of cash and trade rows to Beancount transactions."""

    transactions = []
    for group in groups:
        dtime, cash_rows, trade_rows = group
        if 0:
            PrintGroup(group)

        # Attempt to match up each cash row to each trade rows. We assert that
        # we always find only two situations: N:N matches, where we can pair up
        # the transactions, and 1:n matches (for options strategies) where the
        # fees will be inserted on one of the resulting transactions.
        subgroups = []
        if len(cash_rows) == 1:
            subgroups.append((cash_rows, trade_rows))

        elif len(cash_rows) == len(trade_rows):
            # If we have an N:N situation, pair up the two groups by using quantity.
            cash_rows_copy = list(cash_rows)
            for trow in trade_rows:
                for index, crow in enumerate(cash_rows_copy):
                    if crow.quantity == trow.quantity:
                        break
                else:
                    raise ValueError("Could not find cash row matching the quantity of a trade row")
                crow = cash_rows_copy.pop(index)
                subgroups.append(([crow], [trow]))
            if cash_rows_copy:
                raise ValueError("Internal error: residual row after matching.")

        else:
            message = "Impossible to match up cash and trade rows."
            if is_futures:
                raise ValueError(message)
            else:
                logging.warning(message)
                subgroups.append((cash_rows, trade_rows))

        # Process each of the subgroups.
        for cash_rows, trade_rows in subgroups:
            if 0:
                # Debug print.
                for crow in cash_rows:
                    print('C', crow)
                for trow in trade_rows:
                    print('T', trow)
                print()

            # Fetch the multiplier from the description.
            description = cash_rows[0].description
            if trade_rows[0].type == 'FUTURE':
                multiplier = FindMultiplier(description)
            elif trade_rows[0].type in {'CALL', 'PUT'}:
                multiplier = OPTION_CONTRACT_SIZE
            else:
                multiplier = ONE

            # Pick up all the fees from the cash transactions.
            commissions = sum(crow.commissions_fees for crow in cash_rows)
            fees = sum(crow.misc_fees for crow in cash_rows)
            for trow in trade_rows:
                txn = Txn(trow.exec_time,
                          None,
                          trow.adj_order_id,
                          None,
                          None,
                          'TRADE',
                          trow.side,
                          trow.pos_effect,
                          trow.symbol,
                          quote_currency,
                          trow.quantity,
                          multiplier,
                          trow.price,
                          commissions,
                          fees,
                          description,
                          None)
                transactions.append(txn)
                #print(trow)

                # Reset the commnissions so that they are only included on the
                # first leg where relevant.
                commissions = ZERO
                fees = ZERO

    return transactions


#-------------------------------------------------------------------------------
# Prepare all the tables for processing

def PrepareTables(filename: str) -> Dict[str, Table]:
    """Read and prepare all the tables to be joined."""

    # Handlers for each of the sections.
    handlers = {
        'Cash Balance': CashBalance_Prepare,
        'Futures Statements': FuturesStatements_Prepare,
        'Forex Statements': None,
        'Account Order History': None,
        'Account Trade History': AccountTradeHistory_Prepare,
        'Equities': None,
        'Options': None,
        'Futures': None,
        'Futures Options': None,
        'Profits and Losses': None,
        'Forex Account Summary': None,
        'Account Summary': None,
    }

    # Read the CSV file.
    prepared_tables = {}
    with open(filename, encoding='utf8') as infile:
        # Iterate through the sections.
        sections = csv_utils.csv_split_sections_with_titles(csv.reader(infile))
        for section_name, rows in sections.items():
            handler = handlers.get(section_name, None)
            if not handler:
                continue
            header = csv_utils.csv_clean_header(rows[0])
            rows[0] = header
            table = petl.wrap(rows)
            ptable = handler(table)
            if ptable is None:
                continue
            prepared_tables[section_name] = ptable

    return prepared_tables


def CashBalance_Prepare(table: Table) -> Table:
    """Process the cash account statement balance."""
    table = (
        table
        # Remove bottom totals line.
        .select('description', lambda v: v != 'TOTAL')

        # Convert date/time to a single field.
        .addfield('datetime', partial(ParseDateTimePair, 'date', 'time'), index=0)
        .cutout('date', 'time')

        # Convert numbers to Decimal instances.
        .convert(('commissions_fees', 'amount', 'balance'), ToDecimal)

        # Back out the "Misc Fees" field that is missing using consecutive
        # balances.
        .addfieldusingcontext('misc_fees', _ComputeMiscFees)
    )
    return ParseDescription(table)

def _ComputeMiscFees(prev: Record, rec: Record, _: Record) -> Decimal:
    """Compute the Misc Fees backed from balance difference."""
    if rec is None or prev is None:
        return ZERO
    diff_balance = rec.balance - prev.balance
    return diff_balance - ((rec.amount or ZERO) + (rec.commissions_fees or ZERO))


def FuturesStatements_Prepare(table: Table) -> Table:
    table = (
        table
        # Remove bottom totals line.
        .select('description', lambda v: v != 'TOTAL')

        # Convert date/time to a single field.
        .addfield('datetime',
                  partial(ParseDateTimePair, 'exec_date', 'exec_time'), index=0)
        .cutout('exec_date', 'exec_time')
        .convert('trade_date',
                 lambda v: datetime.datetime.strptime(v, '%m/%d/%y').date())

        # Remove dashes from empty fields (making them truly empty).
        .convert(('ref', 'misc_fees', 'commissions_fees', 'amount'), RemoveDashEmpty)

        # Convert numbers to Decimal or integer instances.
        .convert(('misc_fees', 'commissions_fees', 'amount', 'balance'), ToDecimal)
        .convert('ref', lambda v: int(v) if v else 0)
    )
    return ParseDescription(table)


def ForexStatements_Prepare(table: Table) -> Table:
    return []


def AccountTradeHistory_Prepare(table: Table) -> Table:
    """Prepare the account trade history table."""

    table = (
        table

        # Remove empty columns.
        .cutout('col0')

        # Convert date/time fields to objects.
        .convert('exec_time', lambda string: datetime.datetime.strptime(
            string, '%m/%d/%y %H:%M:%S') if string else None)

        # Fill in missing values.
        .filldown('exec_time')
        .convert(('spread', 'order_type', 'order_id'), lambda v: v or None)
        .filldown('spread', 'order_type', 'order_id')

        # Convert numbers to Decimal instances.
        .convert(('qty', 'price', 'strike'), ToDecimal, pass_row=True)

        # Convert pos effect to single word naming.
        .convert('pos_effect', lambda r: 'OPENING' if 'TO OPEN' else 'CLOSING')

        # Convert order ids to integers (because they area).
        .convert('order_id', lambda v: int(v) if v else 0)

        # Normalize and fixup the symbols to remove the multiplier and month
        # string. '/CLK21 1/1000 MAY 21' is redundant.
        .rename('symbol', 'orig_symbol')
        .addfield('symbol', lambda r: r.orig_symbol.split()[0])

        # Apply symbol changes.
        .convert('symbol', lambda v: SYMBOL_NAME_CHANGES.get(v, v))
    )

    return table


def ParseDateTimePair(date_field: str, time_field: str, rec: Record) -> datetime.date:
    """Parse a pair of date and time fields."""
    return datetime.datetime.strptime(
        "{} {}".format(getattr(rec, date_field), getattr(rec, time_field)),
        '%m/%d/%y %H:%M:%S')


def RemoveDashEmpty(value: str) -> str:
    return value if value != '--' else ''


def ToDecimal(value: str, row=None) -> Union[Decimal, str]:
    # Decimalize bond prices.
    if re.search(r"''", value):
        if row is None:
            raise ValueError("Contract type is needed to determine fraction.")
        match = re.match(r"(\d+)''(\d+)", value)
        if not match:
            raise ValueError("Invalid bond price: {}".format(value))
        # For Treasuries, options quote in 64th's while outrights in 32nd's.
        divisor = 32 if row.type == 'FUTURE' else 64
        dec = Decimal(match.group(1)) + Decimal(match.group(2))/divisor
        #print(value, "->", dec, row.type)
        return dec
    else:
        # Regular prices.
        return Decimal(value.replace(',', '')) if value else ZERO


#-------------------------------------------------------------------------------
# Inference from descriptions

def ParseDescription(table: Table) -> Table:
    """Parse description to synthesize the symbol for later, if present.
    This also adds missing entries.
    """
    return (table
            # Clean up uselesss prefixed from the descriptions.
            .convert('description', CleanDescriptionPrefixes)

            # Parse the description string and insert new columns.
            .addfield('_desc', _ParseDescriptionRecord)
            .addfield('symbol', lambda r: r._desc.get('symbol', ''))
            .addfield('strategy', lambda r: r._desc.get('strategy', ''))
            .addfield('quantity', lambda r: r._desc.get('quantity', ''))
            .cutout('_desc'))



def _ParseDescriptionRecord(row: Record) -> Dict[str, Any]:
    """Parse the description field to a dict."""
    if row.type == 'TRD':
        return _ParseTradeDescription(row.description)
    if row.type == 'RAD':
        if row.description.startswith('REMOVAL OF OPTION'):
            return _ParseExpirationDescription(row.description)
    if row.type == 'DOI':
        if re.match('.* DIVIDEND', row.description):
            return _ParseDividendDescription(row.description)
    return {}


def _ParseTradeDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of a trade."""

    regexp = "".join([
        "(?P<side>BOT|SOLD) ",
        "(?P<quantity>[+-]?[0-9.,]+) ",
        "(?P<rest>.*?)",
        "(?P<price> @-?[0-9.]+)?",
        "(?P<venue> [A-Z]+(?: GEMINI)?)?",
        "$",
    ])
    match = re.match(regexp, description)
    assert match, description
    matches = match.groupdict()
    matches['side'] = 'BUY' if matches['side'] == 'BOT' else 'SELL'
    matches['quantity'] = abs(ToDecimal(matches['quantity']))
    quantity = matches['quantity']
    matches['price'] = ToDecimal(matches['price'].lstrip(" @")) if matches['price'] else ''
    matches['venue'] = matches['venue'].lstrip() if matches['venue'] else ''
    rest = matches.pop('rest')

    underlying = "(?P<underlying>/?[A-Z0-9]+)(?::[A-Z]+)?"
    underlying2 = "(?P<underlying2>/?[A-Z0-9]+)(?::[A-Z]+)?"
    details = "(?P<details>.*)"

    # Standard Options strategies.
    # 'VERTICAL SPY 100 (Weeklys) 8 JAN 21 355/350 PUT'
    # 'IRON CONDOR NFLX 100 (Weeklys) 5 FEB 21 502.5/505/500/497.5 CALL/PUT'
    # 'CONDOR NDX 100 16 APR 21 [AM] 13500/13625/13875/13975 CALL"
    # 'BUTTERFLY GS 100 (Weeklys) 5 FEB 21 300/295/290 PUT'
    # 'VERT ROLL NDX 100 (Weeklys) 29 JAN 21/22 JAN 21 13250/13275/13250/13275 CALL'
    # 'DIAGONAL SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM] 3990/3995 CALL'
    # 'CALENDAR SPY 100 16 APR 21/19 MAR 21 386 PUT'
    # 'STRANGLE NVDA 100 (Weeklys) 1 APR 21 580/520 CALL/PUT'
    # 'COVERED LIT 100 16 APR 21 64 CALL/LIT'
    match = re.match(
        f"(?P<strategy>"
        f"COVERED|VERTICAL|BUTTERFLY|VERT ROLL|DIAGONAL|CALENDAR|STRANGLE"
        f"|CONDOR|IRON CONDOR) {underlying} {details}", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # Custom options combos.
    # '2/2/1/1 ~IRON CONDOR RUT 100 16 APR 21 [AM] 2230/2250/2150/2055 CALL/PUT'
    # '1/-1/1/-1 CUSTOM SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM]/19 MAR 21/19 MAR 21 3990/3980/4000/4010 CALL/CALL/CALL/CALL @-.80'
    # '5/-4 CUSTOM SPX 100 16 APR 21 [AM]/16 APR 21 [AM] 3750/3695 PUT/PUT'
    match = re.match(
        f"(?P<shape>-?\d+(?:/-?\d+)*) (?P<strategy>~IRON CONDOR|CUSTOM) "
        f"{underlying} {details}", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # Futures calendars.
    match = re.match(
        f"(?P<strategy>FUT CALENDAR) {underlying}-{underlying2}", rest)
    if match:
        sub = match.groupdict()
        # Note: Return the front month instrument as the underlying.
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # Single option.
    match = re.match(f"{underlying} {details}", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'SINGLE', 'quantity': quantity, 'symbol': sub['underlying']}

    # 'GAMR 100 16 APR 21 100 PUT'  (-> SINGLE)
    match = re.match(f"{underlying} \d+ {details}", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # Regular stock or future.
    # 'EWW'
    match = re.fullmatch(f"{underlying}", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'OUTRIGHT', 'quantity': quantity, 'symbol': sub['underlying']}

    message = "Unknown description: '{}'".format(description)
    raise ValueError(message)


# NOTE(blais): We don't bother with this anymore; this is now unused, kept
# around just in case we need to revive the complex parsing of the description.
#
# Only the strategy and underlying need be pulled out of the trade description
# for disambiguation with the "Account Trade History" table.
def _BreakDownTradeDescription(description: str) -> Dict[str, Any]:
    """A complex breaking down of the trade description back into its components."""

    # Pieces of regexps used below in matching the different strategies.
    details = "(?P<instrument>.*)"
    underlying = "(?P<underlying>/?[A-Z0-9]+(?::[A-Z]+)?)"
    underlying2 = "(?P<underlying2>/?[A-Z0-9]+(?::[A-Z]+)?)"
    multiplier = "(?P<multiplier>(?:1/)?[0-9]+)"
    suffix = "(?P<suffix>\([A-Za-z]+\))"
    expdate_equity = "\d{1,2} [A-Z]{3} \d{1,2}(?: (?:\[[A-Z+]\]))?"
    expdate_futures = f"[A-Z]{3} \d{1,2}(?: (?:\(EOM\)))? {underlying}"
    expdatef = f"(?P<expdate>{expdate})?"
    strike = "[0-9.]+"
    pc = "(?:PUT|CALL)"
    putcall = "(?P<putcall>PUT|CALL)"
    putcalls = "(?P<putcalls>PUT/CALL|CALL/PUT)"
    size = "[+-]?[0-9,.]+"

    # NOTE: You could easily handle each of the strategies below and expand to
    # all the options legs, like this:
    #   strikes = [ToDecimal(strike) for strike in submatches.pop('strikes').split('/')]
    #   options = []
    #   for strike in strikes:
    #       option = submatches.copy()
    #       option['strike'] = strike
    #       options.append(option)
    # or
    #    matches['expiration'] = parser.parse(matches['expiration']).date()
    #    matches['strike'] = Decimal(matches['strike'])
    #    matches['multiplier'] = Decimal(matches['multiplier'])
    #    matches['quantity'] = Decimal(matches['quantity'])

    # 'VERTICAL SPY 100 (Weeklys) 8 JAN 21 355/350 PUT'
    match = re.match(f"(?P<strategy>VERTICAL) {underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'IRON CONDOR NFLX 100 (Weeklys) 5 FEB 21 502.5/505/500/497.5 CALL/PUT'
    match = re.match(f"(?P<strategy>IRON CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'CONDOR NDX 100 16 APR 21 [AM] 13500/13625/13875/13975 CALL"
    match = re.match(f"(?P<strategy>CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # '2/2/1/1 ~IRON CONDOR RUT 100 16 APR 21 [AM] 2230/2250/2150/2055 CALL/PUT'
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>~IRON CONDOR) "
                     f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # '1/-1/1/-1 CUSTOM SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM]/19 MAR 21/19 MAR 21 3990/3980/4000/4010 CALL/CALL/CALL/CALL @-.80'
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}/{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc}/{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "4", 'quantity': quantity, 'symbol': sub['underlying']}

    # '5/-4 CUSTOM SPX 100 16 APR 21 [AM]/16 APR 21 [AM] 3750/3695 PUT/PUT'
    match = re.match(f"(?P<size>{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "2", 'quantity': quantity, 'symbol': sub['underlying']}

    # 'BUTTERFLY GS 100 (Weeklys) 5 FEB 21 300/295/290 PUT'
    match = re.match(f"(?P<strategy>BUTTERFLY) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'VERT ROLL NDX 100 (Weeklys) 29 JAN 21/22 JAN 21 13250/13275/13250/13275 CALL'
    match = re.match(f"(?P<strategy>VERT ROLL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'DIAGONAL SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM] 3990/3995 CALL'
    match = re.match(f"(?P<strategy>DIAGONAL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'CALENDAR SPY 100 16 APR 21/19 MAR 21 386 PUT'
    match = re.match(f"(?P<strategy>CALENDAR) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'STRANGLE NVDA 100 (Weeklys) 1 APR 21 580/520 CALL/PUT'
    match = re.match(f"(?P<strategy>STRANGLE) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'COVERED LIT 100 16 APR 21 64 CALL/LIT'
    match = re.match(f"(?P<strategy>COVERED) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}) {putcall}/{underlying2}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying2']}

    # 'GAMR 100 16 APR 21 100 PUT'  (-> SINGLE)
    match = re.match(f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'SINGLE', 'quantity': quantity, 'symbol': sub['underlying']}

    # 'EWW'
    match = re.match(f"{underlying}$$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'EQUITY', 'quantity': quantity, 'symbol': sub['underlying']}

    message = "Unknown description: '{}'".format(description)
    raise ValueError(message)


def _ParseExpirationDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of an expiration."""
    regexp = "".join([
        "REMOVAL OF OPTION DUE TO EXPIRATION ",
        "(?P<quantity>[+-]?[0-9.]+) ",
        "(?P<underlying>[A-Z/:]+) ",
        "(?P<multiplier>\d+) ",
        "(?P<suffix>\(.*\) )?",
        "(?P<expiration>\d+ [A-Z]{3} \d+) ",
        "(?P<strike>[0-9.]+) ",
        "(?P<side>PUT|CALL)",
    ])
    match = re.match(regexp, description)
    assert match, description
    matches = match.groupdict()
    matches['expiration'] = parser.parse(matches['expiration']).date()
    matches['strike'] = Decimal(matches['strike'])
    matches['multiplier'] = Decimal(matches['multiplier'])
    matches['quantity'] = Decimal(matches['quantity'])
    return {'strategy': 'EXPIRATION',
            'quantity': Decimal('0'),
            'symbol': matches['underlying']}


def _ParseDividendDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of an expiration."""
    match = re.match("ORDINARY (?P<strategy>DIVIDEND)~(?P<symbol>[A-Z0-9]+)", description)
    assert match, description
    matches = match.groupdict()
    matches['quantity'] = Decimal('0')
    return matches


def CleanDescriptionPrefixes(string: str) -> str:
    return re.sub('(WEB:(AA_[A-Z]+|WEB_GRID_SNAP)|tAndroid) ', '', string)



if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Ameritrade:Main', config={
        'currency'            : 'USD',
        'asset_cash'          : 'Assets:US:Ameritrade:Main:Cash',
        'asset_money_market'  : 'Assets:US:Ameritrade:Main:MMDA1',
        'asset_position'      : 'Assets:US:Ameritrade:Main:{symbol}',
        'option_position'     : 'Assets:US:Ameritrade:Main:Options',
        'asset_forex'         : 'Assets:US:Ameritrade:Forex',
        'futures_contracts'   : 'Assets:US:Ameritrade:Futures:Contracts',
        'futures_options'     : 'Assets:US:Ameritrade:Futures:Options',
        'futures_margin'      : 'Assets:US:Ameritrade:Futures:Margin',
        'futures_cash'        : 'Assets:US:Ameritrade:Futures:Cash',
        'futures_pnl'         : 'Income:US:Ameritrade:Futures:PnL',
        'futures_miscfees'    : 'Expenses:Financial:Fees',
        'futures_commissions' : 'Expenses:Financial:Commissions',
        'fees'                : 'Expenses:Financial:Fees',
        'commission'          : 'Expenses:Financial:Commissions',
        'interest'            : 'Income:US:Ameritrade:Main:Interest',
        'dividend_nontax'     : 'Income:US:Ameritrade:Main:{symbol}:Dividend:NoTax',
        'dividend'            : 'Income:US:Ameritrade:Main:{symbol}:Dividend',
        'adjustment'          : 'Income:US:Ameritrade:Main:Misc',
        'pnl'                 : 'Income:US:Ameritrade:Main:PnL',
        'transfer'            : 'Assets:US:TD:Checking',
        'third_party'         : 'Assets:US:Other:Cash',
        'opening'             : 'Equity:Opening-Balances',
    })
    testing.main(importer)
