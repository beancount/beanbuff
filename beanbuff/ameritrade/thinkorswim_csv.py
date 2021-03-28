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
from typing import Any, Dict, Union, List
from decimal import Decimal
from functools import partial

from dateutil import parser
import petl
petl.config.look_style = 'minimal'

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


OPTION_CONTRACT_SIZE = 100
Table = petl.Table
Record = petl.Record
debug = False
Config = Any


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

        cash_full = tables['Cash Balance']
        trades_equity = (tables['Account Trade History']
                         .addfield('quantity', lambda r: abs(r.qty)))

        # Split the cash statement between simple cash effects vs. trades,
        # expirations and dividend events.
        cash_other = cash_full.select(lambda r: not r.strategy)
        cash_trades = cash_full.select(lambda r: bool(r.strategy))

        # Check that the non-trade cash statement transactions have no overlap
        # whatsoever with the trades on.
        keyed_cash = cash_other.aggregate('datetime', list)
        keyed_trades = trades_equity.aggregate('exec_time', list)
        joined = petl.join(keyed_cash, keyed_trades,
                           lkey='datetime', rkey='exec_time',
                           lprefix='cash', rprefix='trade')
        if joined.nrows() != 0:
            raise ValueError("Cash statement table contains trade data: {}".format(joined))

        # Pair up the trades from the cash statement with the trades from the
        # equity trades table.
        keyed_cash_trades = cash_trades.aggregate(('datetime', 'symbol', 'quantity'), list)
        keyed_trades = trades_equity.aggregate(('exec_time', 'symbol', 'quantity'), list)
        joined_trades = petl.join(keyed_cash_trades, keyed_trades,
                                  lkey=('datetime', 'symbol', 'quantity'),
                                  rkey=('exec_time', 'symbol', 'quantity'),
                                  lprefix='c_', rprefix='t_')
        for datetime, symbol, quantity, cash_rows, trade_rows in joined_trades.skip(1):
            print(datetime)
            for row in cash_rows:
                print(row)
            for row in trade_rows:
                print(row)
            print()

            # Check for collisions against the unique key.
            #
            # Please note that linked orders sent through the Pairs Trader tool
            # will have consecutive ids and the same date/time, but we
            # disambiguate them using hte underlying in the join key.
            #
            # Sometimes we have multiple order ids split up with equivalent
            # trades and so if the descriptions match up exactly we accept
            # those.
            order_ids = {row.order_id for row in trade_rows}
            descriptions = {row.description for row in cash_rows}
            if len(order_ids) != 1 and len(descriptions) != 1:
                message = ("Conflict: More than a single order matched a "
                           "cash line: {}".format(order_ids))
                logging.error(message)
                raise ValueError(message)

        # new_entries = []
        # if entries:
        #     new_entries.extend(entries)
        # return new_entries
        #return process_cash(sections['Cash Balance'], filename, self.config, flag=self.FLAG)


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


def ParseDateTimePair(date_field: str, time_field: str, rec: Record) -> datetime.date:
    """Parse a pair of date and time fields."""
    return datetime.datetime.strptime(
        "{} {}".format(getattr(rec, date_field), getattr(rec, time_field)),
        '%m/%d/%y %H:%M:%S')



def ParseDescription(row: Record) -> Dict[str, Any]:
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

    # Pieces of regexps used below in matching the different strategies.
    underlying = "(?P<underlying>[A-Z]+)"
    underlying2 = "(?P<underlying2>[A-Z]+)"
    multiplier = "(?P<multiplier>[0-9]+)"
    suffix = "(?P<suffix>\([A-Za-z]+\))"
    expdate = "\d+ [A-Z]{3} \d+(?: \[[A-Z]+\])?"
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

    # VERTICAL SPY 100 (Weeklys) 8 JAN 21 355/350 PUT
    match = re.match(f"(?P<strategy>VERTICAL) {underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # IRON CONDOR NFLX 100 (Weeklys) 5 FEB 21 502.5/505/500/497.5 CALL/PUT
    match = re.match(f"(?P<strategy>IRON CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # CONDOR NDX 100 16 APR 21 [AM] 13500/13625/13875/13975 CALL
    match = re.match(f"(?P<strategy>CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 2/2/1/1 ~IRON CONDOR RUT 100 16 APR 21 [AM] 2230/2250/2150/2055 CALL/PUT
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>~IRON CONDOR) "
                     f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 1/-1/1/-1 CUSTOM SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM]/19 MAR 21/19 MAR 21 3990/3980/4000/4010 CALL/CALL/CALL/CALL @-.80
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}/{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc}/{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "4", 'quantity': quantity, 'symbol': sub['underlying']}

    # 5/-4 CUSTOM SPX 100 16 APR 21 [AM]/16 APR 21 [AM] 3750/3695 PUT/PUT
    match = re.match(f"(?P<size>{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "2", 'quantity': quantity, 'symbol': sub['underlying']}

    # BUTTERFLY GS 100 (Weeklys) 5 FEB 21 300/295/290 PUT
    match = re.match(f"(?P<strategy>BUTTERFLY) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # VERT ROLL NDX 100 (Weeklys) 29 JAN 21/22 JAN 21 13250/13275/13250/13275 CALL
    match = re.match(f"(?P<strategy>VERT ROLL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # DIAGONAL SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM] 3990/3995 CALL
    match = re.match(f"(?P<strategy>DIAGONAL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # CALENDAR SPY 100 16 APR 21/19 MAR 21 386 PUT
    match = re.match(f"(?P<strategy>CALENDAR) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # STRANGLE NVDA 100 (Weeklys) 1 APR 21 580/520 CALL/PUT
    match = re.match(f"(?P<strategy>STRANGLE) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # COVERED LIT 100 16 APR 21 64 CALL/LIT
    match = re.match(f"(?P<strategy>COVERED) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}) {putcall}/{underlying2}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying2']}

    # GAMR 100 16 APR 21 100 PUT
    match = re.match(f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'SINGLE', 'quantity': quantity, 'symbol': sub['underlying']}

    # EWW
    match = re.match(f"{underlying}$$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'EQUITY', 'quantity': quantity, 'symbol': sub['underlying']}

    message = "Unknown description: {}".format(description)
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
    return re.sub('(WEB:AA_[A-Z]+|tAndroid) ', '', string)


def RemoveDashEmpty(value: str) -> str:
    return value if value != '--' else ''


def ToDecimal(value: str) -> Union[Decimal, str]:
    return Decimal(value.replace(',', '')) if value else ''


def TranslateInstrument(inst_string: str):
    # Simple option.
    print(inst_string)
    match = re.match(r"([A-Z0-9]+) (\d+)( \([A-Za-z]+\))? (\d+ [A-Z]+ \d+) (.*) (PUT|CALL)",
                     inst_string)
    print(match.groups())


    # Simple future.
    futsym = r"/([A-Z]{2}[A-Z0-9]+)(?::X(?:CME|CEC|NYM))?( .*)?"
    match = re.match(fr"{futsym}( .*)?", inst_string)
    assert match, "Invalid instrument name from: {}".format(inst_string)
    underlying = "{}".format(match.group(1))
    opt_string = match.group(2) and match.group(2).lstrip()

    # Option on future.
    option = ''
    if opt_string:
        match = re.match(
            fr"\d/(\d+) ([A-Z]{{3}}) (\d+) (\(EOM\) )?{futsym}(/{futsym})? ([0-9.]+) (CALL|PUT)",
            opt_string)
        if match:
            optsym = match.group(5)
            # TODO(blais): Include the second one in the pair too.
            letter = 'C' if match.group(9) == 'CALL' else 'P'
            strike = match.group(8)
            option = f"{optsym}{letter}{strike}".format(match.group(7))

    return underlying, option


#-------------------------------------------------------------------------------
# Cash Balance

def CashBalance_Prepare(table: Table) -> Table:
    """Process the cash account statement balance."""

    table = (
        table

        # Remove bottom totals line.
        .select('description', lambda v: v != 'TOTAL')
        # Clean up uselesss prefixed from the descriptions.
        .convert('description', CleanDescriptionPrefixes)

        # Convertd date/time to a single field.
        .addfield('datetime', partial(ParseDateTimePair, 'date', 'time'), index=0)
        .cutout('date', 'time')

        # Convert numbers to Decimal instances.
        .convert(('commissions_fees', 'amount', 'balance'), ToDecimal)

        # Back out the "Misc Fees" field that is missing using consecutive
        # balances.
        .addfieldusingcontext('misc_fees', ComputeMiscFees)

        # Parse to synthesize the symbol for later, if present.
        .addfield('do', ParseDescription)
        .addfield('symbol', lambda r: r.do.get('symbol', ''))
        .addfield('strategy', lambda r: r.do.get('strategy', ''))
        .addfield('quantity', lambda r: r.do.get('quantity', ''))
        .cutout('do')
    )

    return table


def ComputeMiscFees(prev: Record, rec: Record, _: Record) -> Decimal:
    """Compute the Misc Fees backed from balance difference."""
    if rec is None or prev is None:
        return ZERO
    diff_balance = rec.balance - prev.balance
    return diff_balance - ((rec.amount or ZERO) + (rec.commissions_fees or ZERO))


#-------------------------------------------------------------------------------
# Futures Statements

def FuturesStatements_Prepare(table: Table) -> Table:
    table = (
        table

        # Remove bottom totals line.
        .select('description', lambda v: v != 'TOTAL')
        # Clean up uselesss prefixed from the descriptions.
        .convert('description', CleanDescriptionPrefixes)

        # Convertd date/time to a single field.
        .addfield('datetime',
                  partial(ParseDateTimePair, 'exec_date', 'exec_time'), index=0)
        .cutout('exec_date', 'exec_time')
        .convert('trade_date',
                 lambda v: datetime.datetime.strptime(v, '%m/%d/%y').date())

        # .select('description',
        #         lambda v: not re.match(r'Cash balance at the start of business day', v))


        # Remove dashes from empty fields (making them truly empty).
        .convert(('ref', 'misc_fees', 'commissions_fees', 'amount'), RemoveDashEmpty)

        # Convert numbers to Decimal instances.
        .convert(('misc_fees', 'commissions_fees', 'amount', 'balance'), ToDecimal)
    )

    return table


#-------------------------------------------------------------------------------
# Forex Statements

def ForexStatements_Prepare(table: Table) -> Table:
    return []


#-------------------------------------------------------------------------------
# Account Order History
#-------------------------------------------------------------------------------
# Account Trade History

def AccountTradeHistory_Prepare(table: Table) -> Table:
    """Prepare the account trade history table."""

    table = (
        table

        # Remove empty columns.
        .cutout('col0')

        # Convert date/time fields to objects.
        .convert('exec_time', lambda string: datetime.datetime.strptime(
            string, '%m/%d/%y %H:%M:%S'))

        # Fill in missing values.
        .filldown('exec_time')
        .convert(('spread', 'order_type', 'order_id'), lambda v: v or None)
        .filldown('spread', 'order_type', 'order_id')

        # Convert numbers to Decimal instances.
        .convert(('qty', 'price', 'strike'), ToDecimal)

        # Convert pos effect to single word naming.
        .convert('pos_effect', lambda r: 'OPENING' if 'TO OPEN' else 'CLOSING')

        # Convert order ids to integers (because they area).
        .convert('order_id', int)
    )

    return table


#-------------------------------------------------------------------------------
# Equities
#-------------------------------------------------------------------------------
# Options
#-------------------------------------------------------------------------------
# Futures
#-------------------------------------------------------------------------------
# Futures Options
#-------------------------------------------------------------------------------
# Profits and Losses
#-------------------------------------------------------------------------------
# Forex Account Summary
#-------------------------------------------------------------------------------
# Account Summary





# def FuturesStatements(table, filename, config):
#     table = (
#         _PrepareFuturesStatements(table, 'exec_date', 'exec_time')
#         .convert('trade_date',
#                  lambda v: datetime.datetime.strptime(v, '%m/%d/%y').date()))
#
#     new_entries = []
#     balances = collections.defaultdict(Inventory)
#     for index, row in enumerate(table.records()):
#         handler = _TRANSACTION_HANDLERS[row.type]
#         entries = handler(row, filename, index, config, balances)
#         if entries:
#             insert = (new_entries.extend
#                       if isinstance(entries, list)
#                       else new_entries.append)
#             insert(entries)
#
#     return new_entries



def OnBalance(row: Record, filename: str, index: int, config: Config, balances: Inventory) -> data.Entries:
    meta = data.new_metadata(filename, index)
    balance = Amount(row.balance, config['currency'])
    return data.Balance(meta, row.trade_date, config['futures_cash'],
                        balance, None, None)


def OnFuturesSWeep(row: Record, filename: str, index: int, config: Config, balances: Inventory) -> data.Entries:
    if row.amount == ZERO:
        return
    meta = data.new_metadata(filename, index)
    amount = Amount(row.amount, config['currency'])
    return data.Transaction(
        meta, row.trade_date, flags.FLAG_OKAY,
        None, row.description, set(), set(), [
            data.Posting(config['futures_cash'], amount, None, None, None, None),
            data.Posting(config['asset_cash'], -amount, None, None, None, None),
        ])


# Contract multipliers.
_MULTIPLIERS = {
    "NQ": 20,
    "QNE": 20,
    "CL": 1000,
    "GC": 100,
}


def OnTrade(row: Record, filename: str, index: int, config: Config, balances: Inventory) -> data.Entries:
    assert row.trade_date == row.datetime.date()
    if row.strategy == 'FUTURE':
        return OnFuturesTrade(row, filename, index, config, balances)
    else:
        return OnFuturesOptionTrade(row, filename, index, config, balances)


def GetMultiplier(row, config):
    """Inflate the price with the multiplier."""
    match = re.match("([A-Z]{1,3})[FGHJKMNQUVXZ]2[0-9]", row.underlying)
    multiplier = _MULTIPLIERS[match.group(1)] if match else 1
    mult_price = row.price * multiplier
    posting_meta = {'contract': Amount(row.price, config['currency'])}
    return mult_price, posting_meta


def OnFuturesTrade(row: Record, filename: str, index: int, config: Config, balances: Inventory) -> data.Entries:
    currency = config['currency']
    mult_price, posting_meta = GetMultiplier(row, config)
    meta = data.new_metadata(filename, index)
    units = Amount(row.quantity, row.underlying)

    # NOTE(blais): The trade matching is at average cost from TD, so we use the
    # "NONE" method for now. No need to check for "row.side == 'BUY'"
    if True:
        cost = position.CostSpec(mult_price, None, currency, None, None, False)
        price = None
        margin = Amount(-row.quantity * mult_price, currency)
    else:
        cost = position.CostSpec(None, None, currency, None, None, False)
        price = Amount(mult_price, currency)
        margin = Amount(MISSING, currency)

    # P/L only, and only on sales.
    cash_effect = Inventory()

    links = {'td-ref-{}'.format(row.ref)}
    txn = data.Transaction(
        meta, row.datetime.date(), flags.FLAG_OKAY,
        None, row.description, set(), set(), [
            data.Posting(config['futures_contracts'], units, cost, price, None, posting_meta),
            data.Posting(config['futures_margin'], margin, None, None, None, None),
        ])

    if row.amount:
        amount = Amount(-row.amount or ZERO, currency)
        cash_effect.add_amount(amount)
        txn.postings.append(
            data.Posting(config['futures_pnl'], amount,
                         None, None, None, None))

    if row.commissions_fees:
        commissions = Amount(-row.commissions_fees, currency)
        cash_effect.add_amount(commissions)
        txn.postings.append(
            data.Posting(config['futures_commissions'], commissions,
                         None, None, None, None))
    if row.misc_fees:
        misc_fees = Amount(-row.misc_fees, currency)
        cash_effect.add_amount(misc_fees)
        txn.postings.append(
            data.Posting(config['futures_miscfees'], misc_fees, None,
                         None, None, None))

    for pos in cash_effect:
        txn.postings.append(
            data.Posting(config['futures_cash'], -pos.units, None,
                         None, None, None))

    return txn


def OnFuturesOptionTrade(row: Record, filename: str, index: int, config: Config, balances: Inventory) -> data.Entries:
    currency = config['currency']
    mult_price, posting_meta = GetMultiplier(row, config)
    meta = data.new_metadata(filename, index)
    units = Amount(row.quantity, row.underlying)

    meta = data.new_metadata(filename, index)
    if not row.option:
        logging.error("Could not import: %s; requires multi-table reconciliation.", row)
        return
    units = Amount(row.quantity, row.option)

    # Update the balance of units, keeping track of the position so we can write
    # augmentations and reductions the same way.
    balance = balances[config['futures_options']]
    balance_units = balance.get_currency_units(units.currency)
    is_augmentation = (balance_units.number == ZERO or
                       (balance_units.number * units.number) > ZERO)
    balance.add_amount(units)

    # NOTE(blais): The trade matching is at average cost from TD, so we use the
    # "NONE" method for now. No need to check for "row.side == 'BUY'"
    if is_augmentation:
        cost = position.CostSpec(mult_price, None, currency, None, None, False)
        price = None
    else:
        cost = position.CostSpec(None, None, currency, None, None, False)
        price = Amount(mult_price, currency)

    links = {'td-ref-{}'.format(row.ref)}
    txn = data.Transaction(
        meta, row.datetime.date(), flags.FLAG_OKAY,
        None, row.description, set(), set(), [
            data.Posting(config['futures_options'], units, cost, price, None, posting_meta),
        ])

    if row.commissions_fees:
        commissions = Amount(-row.commissions_fees, currency)
        txn.postings.append(
            data.Posting(config['futures_commissions'], commissions,
                         None, None, None, None))
    if row.misc_fees:
        misc_fees = Amount(-row.misc_fees, currency)
        txn.postings.append(
            data.Posting(config['futures_miscfees'], misc_fees, None,
                         None, None, None))

    cash = Amount(row.amount, currency)
    txn.postings.append(
        data.Posting(config['futures_cash'], cash, None,
                     None, None, None))

    if not is_augmentation:
        txn.postings.append(
            data.Posting(config['futures_pnl'], Amount(MISSING, currency),
                         None, None, None, None))

    return txn

# TODO(blais): Add ref numbers, ^td-?


_TRANSACTION_HANDLERS = {
    'BAL': OnBalance,
    'TRD': OnTrade,
    'FSWP': OnFuturesSWeep,
}



## def _process_cash_balance(table, filename, config):
##     # ['date', 'time', 'type', 'ref', 'description', 'misc_fees', 'commissions_fees', 'amount', 'balance']
##
##     print(table.lookallstr())
##
##     flag='*'
##     new_entries = []
##     cash_currency = config['currency']
##
##     # irows = iter(section)
##     # fieldnames = csv_utils.csv_clean_header(next(irows))
##     # Tuple = collections.namedtuple('Row', fieldnames)
##     # tuples = list(itertools.starmap(Tuple, irows))
##
##     prev_balance = Amount(D(), cash_currency)
##     prev_date = datetime.date(1970, 1, 1)
##     date_format = find_date_format(tuples)
##     for index, row in enumerate(tuples):
##         # Skip the empty balances; these aren't interesting.
##         if re.search('Cash balance at the start of business day', row.description):
##             continue
##
##         # Skip end lines that cannot be parsed.
##         if not row.date:
##             continue
##
##         # Get the row's date and fileloc.
##         fileloc = data.new_metadata(filename, index)
##         date = datetime.datetime.strptime(row.date, date_format).date()
##
##         # Insert some Balance entries every time the day changed.
##         if ((debug and date != prev_date) or
##             (not debug and date.month != prev_date.month)):
##
##             prev_date = date
##             fileloc = data.new_metadata(filename, index)
##             new_entries.append(data.Balance(fileloc, date, config['asset_cash'],
##                                             prev_balance, None, None))
##
##         # Create a new transaction.
##         narration = "({0.type}) {0.description}".format(row)
##         links = set([row.ref]) if hasattr(row, 'ref') else set()
##         entry = data.Transaction(fileloc, date, flag, None, narration, set(), links, [])
##
##         amount_ = convert_number(row.amount)
##         if row.type != 'TRD':
##             assert not get_one_of(row, 'fees', 'misc_fees'), row
##             assert not get_one_of(row, 'commissions', 'commissions_fees'), row
##
##         balance = Amount(convert_number(row.balance), cash_currency)
##
##         if row.type == 'EFN':
##             assert re.match(r'CLIENT REQUESTED ELECTRONIC FUNDING (RECEIPT|DISBURSEMENT) \(FUNDS NOW\)',
##                             row.description)
##             data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##             data.create_simple_posting(entry, config['transfer'], -amount_, cash_currency)
##
##         elif row.type == 'RAD':
##             if re.match('STOCK SPLIT', row.description):
##                 # Ignore the stock splits for now, because they don't specify by how much.
##                 pass
##             elif re.match('(MONEY MARKET INTEREST|MM Purchase)', row.description):
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['interest'], -amount_, cash_currency)
##             elif save(re.match('REMOVAL OF OPTION DUE TO (EXPIRATION|ASSIGNMENT) (-?[0-9\.]+) (.*)', row.description)):
##                 amount_ = D(save.value.group(2)) * OPTION_CONTRACT_SIZE
##                 symbol = match_option_name(save.value.group(3))
##                 account_ = config['option_position'].format(symbol=symbol)
##                 posting = data.Posting(account_,
##                                        Amount(amount_, symbol),
##                                        position.Cost(ZERO, cash_currency, None, None),
##                                        Amount(ZERO, cash_currency),
##                                        None, None)
##                 entry.postings.append(posting)
##                 #data.create_simple_posting(entry, config['asset_cash'], ZERO, cash_currency)
##                 data.create_simple_posting(entry, config['pnl'], None, None)
##             elif save(re.match('MANDATORY - NAME CHANGE', row.description)):
##                 pass # Ignore this.
##             else:
##                 assert re.match('(MONEY MARKET INTEREST|MM Purchase)', row.description), row.description
##
##         elif row.type == 'JRN':
##             if re.match('TRANSFER (TO|FROM) FOREX ACCOUNT', row.description):
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['asset_forex'], -amount_, cash_currency)
##             elif re.match('INTRA-ACCOUNT TRANSFER', row.description):
##                 assert row.amount
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['interest'], -amount_, cash_currency)
##             elif re.match('MARK TO THE MARKET', row.description):
##                 pass # Do nothing.
##             else:
##                 assert False, row
##
##         elif row.type == 'DOI':
##             sym_match = re.search('~(.*)$', row.description)
##             assert sym_match, "Error: Symbol not found for dividend"
##             symbol = sym_match.group(1)
##
##             if re.match('(ORDINARY DIVIDEND|LONG TERM GAIN DISTRIBUTION|SHORT TERM CAPITAL GAINS)', row.description):
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['dividend'].format(symbol=symbol), -amount_, cash_currency)
##
##             elif re.match('NON-TAXABLE DIVIDENDS', row.description):
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['dividend_nontax'].format(symbol=symbol), -amount_, cash_currency)
##
##             elif re.match('FREE BALANCE INTEREST ADJUSTMENT', row.description):
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['adjustment'], -amount_, cash_currency)
##
##             else:
##                 assert False, row.description
##
##         elif row.type == 'WIN':
##             assert re.match('THIRD PARTY|WIRE INCOMING', row.description), row
##             data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##             data.create_simple_posting(entry, config['third_party'], -amount_, cash_currency)
##
##         elif row.type == 'TRD':
##             if save(re.match(r'(?P<prefix>WEB:[^ ]+ )'
##                              r'?(?P<side>BOT|SOLD) '
##                              r'(?P<qty>[+\-0-9]+) '
##                              r'(?P<inst>.+) '
##                              r'@(?P<price>[0-9\.]+)', row.description)):
##                 quantity = D(save.value.group('qty'))
##                 isbuy = save.value.group('side') == 'BOT'
##                 price_number = D(save.value.group('price'))
##                 symbol = save.value.group('inst')
##
##             elif save(re.match(r'(?P<side>BOT|SOLD) '
##                                r'(?P<qty>[+\-0-9.]+) '
##                                r'(?P<inst>.+) '
##                                r'UPON (?:OPTION ASSIGNMENT|TRADE CORRECTION)', row.description)):
##                 quantity = D(save.value.group('qty'))
##                 isbuy = save.value.group('side') == 'BOT'
##                 symbol = save.value.group('inst')
##
##                 # Unfortunately we have to back out the price from the amount
##                 # because it is not in the description.
##                 total_amount = D(row.amount) #- D(row.commissions_fees) - D(row.misc_fees)
##                 price_number = abs(total_amount / quantity).quantize(total_amount)
##             else:
##                 assert False, row
##
##             if re.match(r"[A-Z0-9]+$", symbol):
##                 account_type = 'asset_position'
##             elif save(match_option_name(symbol)):
##                 symbol = save.value
##                 quantity *= OPTION_CONTRACT_SIZE
##                 account_type = 'option_position'
##             else:
##                 assert False, "Invalid symbol: '{}'".format(symbol)
##
##             account_ = config[account_type].format(symbol=symbol)
##             price = Amount(price_number, cash_currency)
##             cost = position.Cost(price.number, price.currency, None, None)
##             units = Amount(D(quantity), symbol)
##             posting = data.Posting(account_, units, cost, None, None, None)
##             if not isbuy:
##                 posting = posting._replace(price=price)
##             entry.postings.append(posting)
##
##             commissions = get_one_of(row, 'commissions', 'commissions_fees')
##             if commissions:
##                 data.create_simple_posting(entry, config['commission'], -D(commissions), cash_currency)
##                 amount_ += D(commissions)
##
##             misc_fees = get_one_of(row, 'fees', 'misc_fees')
##             if misc_fees:
##                 data.create_simple_posting(entry, config['fees'], -D(misc_fees), cash_currency)
##                 amount_ += D(misc_fees)
##
##             data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##             if not isbuy:
##                 data.create_simple_posting(entry, config['pnl'], None, None)
##
##         elif row.type == 'ADJ':
##             if row.description == 'Account Opt In':
##
##                 # If this is the first year, an opt-in probably requires an adjustment.
##                 entry = data.Pad(fileloc, date, config['asset_cash'], config['opening'])
##                 new_entries.append(entry)
##
##                 # And an associated check.
##                 new_entries.append(data.Balance(fileloc, date, config['asset_cash'],
##                                                 balance, None, None))
##
##                 continue # No entry.
##
##             elif row.description == 'Courtesy Credit':
##                 data.create_simple_posting(entry, config['asset_cash'], amount_, cash_currency)
##                 data.create_simple_posting(entry, config['dividend_nontax'], -amount_, cash_currency)
##
##         else:
##             raise ValueError("Unknown transaction {}".format(row))
##
##         new_entries.append(entry)
##         prev_balance = balance
##
##     return new_entries
##
##
## def find_date_format(tuples):
##     """Classify whether the rows are using the old or the new date format.
##
##     Think-or-swim files appear to have changed date format between 2015-09-06
##     and 2015-10-06.
##
##     Args:
##       tuples: A list of tuples.
##     Returns:
##       A string, the date parsing format.
##     """
##     cols0, cols1 = [], []
##     for row in tuples:
##         match = re.match(r'(\d+)/(\d+)/\d\d', row[0])
##         if match is None:
##             continue
##         col0, col1 = map(int, match.group(1, 2))
##         cols0.append(col0)
##         cols1.append(col1)
##
##     if max(cols0) > 12:
##         assert max(cols1) <= 12
##         return '%d/%m/%y'
##     else:
##         assert max(cols0) <= 12
##         assert max(cols1) > 12
##         return '%m/%d/%y'
##
##
## def convert_number(string):
##     if not string or string == '--':
##         return D()
##     mo = re.match(r'\((.*)\)', string)
##     if mo:
##         sign = -1
##         string = mo.group(1)
##     else:
##         sign = 1
##
##     number = D(re.sub('[\$,]', '', string)) if string != '--' else D()
##     return number * sign
##
##
## def match_option_name(string):
##     "Match against the name of an option (or return None)."
##     match = re.match((r"(?P<symbol>[A-Z0-9]+) "
##                       r"(?P<units>[0-9]+) "
##                       r"(?P<kind>\(.*\) )?"
##                       r"(?P<day>[0-9]+) "
##                       r"(?P<month>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) "
##                       r"(?P<year>[0-9][0-9]) "
##                       r"(?P<strike>[0-9]+) "
##                       r"(?P<type>CALL|PUT)"), string)
##     if match:
##         gmap = match.groupdict()
##         gmap['month'] = "{:02d}".format(
##             "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC".split("|").index(
##                 gmap['month']) + 1)
##         gmap['t'] = 'C' if gmap['type'] == 'CALL' else 'P'
##         return "{symbol}{year}{month}{day}{t}{strike}".format(**gmap)
##
##
## def get_one_of(row, *attributes):
##     for attribute in attributes:
##         if hasattr(row, attribute):
##             return getattr(row, attribute)


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
