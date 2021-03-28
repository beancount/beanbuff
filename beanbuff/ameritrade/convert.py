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
# TODO(blais): Fetch these from the downloadable CME table.
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


# def TranslateInstrument(inst_string: str):
#     # Simple option.
#     match = re.match(r"([A-Z0-9]+) (\d+)( \([A-Za-z]+\))? (\d+ [A-Z]+ \d+) (.*) (PUT|CALL)",
#                      inst_string)
#
#     # Simple future.
#     futsym = r"/([A-Z]{2}[A-Z0-9]+)(?::X(?:CME|CEC|NYM))?( .*)?"
#     match = re.match(fr"{futsym}( .*)?", inst_string)
#     assert match, "Invalid instrument name from: {}".format(inst_string)
#     underlying = "{}".format(match.group(1))
#     opt_string = match.group(2) and match.group(2).lstrip()
#
#     # Option on future.
#     option = ''
#     if opt_string:
#         match = re.match(
#             fr"\d/(\d+) ([A-Z]{{3}}) (\d+) (\(EOM\) )?{futsym}(/{futsym})? ([0-9.]+) (CALL|PUT)",
#             opt_string)
#         if match:
#             optsym = match.group(5)
#             # TODO(blais): Include the second one in the pair too.
#             letter = 'C' if match.group(9) == 'CALL' else 'P'
#             strike = match.group(8)
#             option = f"{optsym}{letter}{strike}".format(match.group(7))
#
#     return underlying, option
