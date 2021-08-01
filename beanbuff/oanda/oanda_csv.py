"""OANDA Corporation transaction detail CSV file importer.

Go to the old transaction detail page, select CSV detail, and and cut-n-paste
the output into a file (you have to do this manually, unfortunately, there is no
option).
"""
import re
import datetime
import collections

from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core import data
from beancount.core import amount
from beancount.ops import compress

from beangulp import csv_utils
from beangulp import testing
from beangulp.importers.mixins import config
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'asset'    : 'Account holding the cash margin',
        'interest' : 'Interest income',
        'pnl'      : 'PnL income',
        'transfer' : 'Other account for wire transfers',
        'limbo'    : "Account used to book against transfer where we don't know",
        'fees'     : 'Wire and API fees',
    }

    matchers = [
        ('mime', 'text/csv'),
        ('content', 'Transaction ID.*Currency Pair.*Pipettes'),
    ]

    def extract(self, file):
        return import_csv_file(file.name, self.config, flag=self.FLAG)


IGNORE_TRANSACTIONS = """
Buy Order
Sell Order
Change Margin
Change Order
Change Trade
Order Cancelled
Order Expired
Order Filled
""".strip().splitlines()

RELEVANT_TRANSACTIONS = """
API Fee
API License Fee
Wire Fee
Buy Market Filled
Close Position
Close Trade
Fund Deposit
Fund Deposit (Transfer)
Fund Deposit (Account Transfer)
Fund Withdrawal
Fund Withdrawal (Transfer)
Fund Withdrawal (Account Transfer)
Fund Fee
Interest
Sell Market Filled
Stop Loss
Stop Loss (Cancelled)
Take Profit
Trade Cancel
Buy Market
Sell Market
FXGlobalTransfer Sent
FXGlobalTransfer Fee
P/L Correction
Interest Correction
Balance Correction
""".strip().splitlines()


def find_changing_types(filename):
    bytype = collections.defaultdict(list)
    with open(filename) as infile:
        for obj in csv_utils.csv_dict_reader(infile):
            txntype = obj['transaction']
            bytype[txntype].append(obj)

    unchanging_types = set(bytype.keys())
    prev_balance = D()
    with open(filename) as infile:
        for obj in csv_utils.csv_dict_reader(infile):
            balance = obj['balance'].strip()
            if balance and balance != prev_balance:
                if obj['transaction'] in unchanging_types:
                    print(obj)
                unchanging_types.discard(obj['transaction'])
                prev_balance = balance

    print("Unchanging types:")
    for txntype in unchanging_types:
        print(txntype)
    print()

    print("Changing types:")
    changing_types = set(bytype.keys()) - unchanging_types
    for txntype in changing_types:
        print(txntype)
    print()


def get_number(obj, aname):
    str_value = obj[aname].strip()
    if str_value:
        return D(str_value.replace(',', ''))
    else:
        return D()


def reset_balance(txntype):
    """Return true if the balance cannot be assumed to be balanced.

    Args:
      txntype: A string, the name of the transaction type.
    Returns:
      A boolean.
    """
    return (txntype == 'Trade Cancel' or
            txntype.startswith('Fund ') or
            txntype.startswith('FXGlobalTransfer ') or
            txntype.startswith('API ') or
            txntype.endswith('Correction') or
            re.search(txntype, 'Wire Fee') or
            re.search(txntype, 'FXGlobalTransfer Fee'))


TOLERANCE = D('0.01')
QS = D('0.01')
QL = D('0.0001')

LINK_FORMAT = 'oanda-{}'

# Ignore everything before this date. OANDA's exporter provides an input for
# entering the beginning date but it looks like it's not working. There was a
# big fuckup in 2007 and they have to make corrections and their accounts don't
# balance properly. It's been a while now, just skip that problem, I have other
# things to do (that being said, it's _almost_ working for that time, so just
# change this if you want it).
FIRST_DATE = datetime.date(2009, 1, 1)

def guess_currency(filename):
    """Try to guess the base currency of the account.
    We use the first transaction with a deposit or something
    that does not involve an instrument."""
    with open(filename) as infile:
        for obj in csv_utils.csv_dict_reader(infile):
            if re.match('[A-Z]+$', obj['currency_pair']):
                return obj['currency_pair']


def oanda_add_posting(entry, account, number, currency):
    units = amount.Amount(number, currency)
    posting = data.Posting(account, units, None, None, None, None)
    entry.postings.append(posting)


def yield_records(filename, config):
    """Yield records for an OANDA file.

    Args:
      filename: a string, the name of the file to parse.
      config: A configuration directory.
    Yields:
      Records of the form:


    """
    # Sort all the lines and compute the dates.
    with open(filename) as infile:
        objiter = csv_utils.csv_dict_reader(infile)
        sorted_objects = reversed(list(objiter))

        # Iterate over all the transactions in the OANDA account.
        prev_balance = None
        other_account = None
        for obj in sorted_objects:
            txntype = obj['type']
            date = datetime.datetime.strptime(obj['time_utc'], '%Y-%m-%d %H:%M:%S').date()

            # Skip everything before supported first date.
            if date < FIRST_DATE:
                continue

            # Ignore certain ones that have no effect on the balance, they just
            # change our positions.
            if txntype in IGNORE_TRANSACTIONS:
                continue
            assert txntype in RELEVANT_TRANSACTIONS, txntype

            # Get the change amounts.
            amount_interest = get_number(obj, 'interest')
            amount_pnl = get_number(obj, 'pl')
            amount_amount = get_number(obj, 'amount')
            amount_other = ZERO

            # The balance reported.
            reported_balance = get_number(obj, 'balance')

            # Compute the new balance and the final amounts.
            if prev_balance is None:
                # For the first line, set the balance to the first reported balance.
                prev_balance = reported_balance - (amount_pnl + amount_interest + amount_other)

            elif reset_balance(txntype):
                # For special unbalancing transactions, check which sign we should
                # be applying.
                reported_change = reported_balance - prev_balance
                for sign in (+1, -1):
                    diff = sign * amount_amount - reported_change
                    if abs(diff) < TOLERANCE:
                        break
                else:
                    raise ValueError("Cannot use straight-up amount, "
                                     "too far from zero: {} {}".format(amount_amount,
                                                                       reported_change))

                amount_other = sign * amount_amount
                amount_pnl = ZERO
                amount_interest = ZERO

                # We will need to assign an account here.
                if 'Fee' in txntype:
                    other_account = config['fees']
                elif 'Transfer' in txntype:
                    other_account = config['transfer']
                else:
                    other_account = config['limbo']
            else:
                # For regular transactions, just use P/L and interest columns.
                amount_other = ZERO

            change = amount_pnl + amount_interest + amount_other
            balance = prev_balance + change

            if 0:
                print("%s | %-16.16s | amount:%16.4f | interest:%16.4f | P/L:%16.4f | change:%16.6f | computed: %16.6f | reported:%16.2f | diff:%16.6f" % (
                    date,
                    txntype,
                    amount_amount,
                    amount_interest,
                    amount_pnl,
                    change,
                    balance,
                    reported_balance,
                    balance - reported_balance))

            # Check that the change updates the balance correctly.
            if abs(balance - reported_balance) > TOLERANCE:
                raise ValueError("Balances don't match: {} != {}".format(reported_balance, balance))

            # Create the transaction.
            narration = '{} - {}'.format(txntype, obj['currency_pair'])

            transaction_link = obj['transaction_link']
            if transaction_link == '0':
                transaction_link = None

            yield (date, obj['transaction_id'], transaction_link, narration,
                   change,
                   amount_pnl, amount_interest, amount_other, other_account,
                   prev_balance)

            # Set the previous blance.
            prev_balance = balance



def import_csv_file(filename, config, do_compress=True, flag='*'):
    new_entries = []

    currency = guess_currency(filename)

    prev_date = datetime.date(1970, 1, 1)
    for lineno, record in enumerate(yield_records(filename, config)):
        (date, transaction_id, transaction_link, narration,
         change,
         amount_pnl, amount_interest, amount_other, other_account,
         prev_balance) = record

        # Insert some Balance entries every month or so.
        if date.month != prev_date.month and prev_balance is not None:
            prev_date = date
            fileloc = data.new_metadata(filename, lineno)
            amount_balance = amount.Amount(prev_balance.quantize(QS), currency)
            new_entries.append(
                data.Balance(fileloc, date, config['asset'], amount_balance, None, None))

        # Create links.
        links = set([LINK_FORMAT.format(transaction_id.strip())])
        if transaction_link:
            links.add(LINK_FORMAT.format(transaction_link.strip()))

        source = data.new_metadata(filename, lineno)
        entry = data.Transaction(source, date, flag, None, narration,
                                 data.EMPTY_SET, links, [])

        # FIXME: Add the rates for transfers
        oanda_add_posting(entry, config['asset'], change.quantize(QL), currency)
        if amount_pnl != ZERO:
            oanda_add_posting(entry, config['pnl'], -amount_pnl.quantize(QL), currency)
        if amount_interest != ZERO:
            oanda_add_posting(entry, config['interest'], -amount_interest.quantize(QL), currency)
        if amount_other != ZERO:
            oanda_add_posting(entry, other_account, -amount_other.quantize(QL), currency)

        if len(entry.postings) < 2:
            continue

        new_entries.append(entry)

        assert len(entry.postings) > 1, printer.format_entry(entry)

    new_entries.sort(key=lambda entry: entry.date)

    if do_compress:
        # Compress all the interest entries for a shorter and cleaner set of
        # imported transactions.
        new_entries = compress.compress(
            new_entries,
            lambda entry: re.search('Interest', entry.narration))

    return new_entries


# Future work:
#
# - Render trades into positions from subaccounts, just like we do for stocks. A
#   large positive number and a large negative number, this should be possible,
#   under e.g. 'Income:US:OANDA:Primary:Positions:EUR_USD'


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:OANDA:Primary', config={
        'asset'    : 'Assets:US:OANDA:Primary',
        'interest' : 'Income:US:OANDA:Primary:Interest',
        'pnl'      : 'Income:US:OANDA:Primary:PnL',
        'transfer' : 'Assets:US:OANDA:Transfer',
        'limbo'    : 'Assets:US:OANDA:Limbo',
        'fees'     : 'Expenses:Financial:Fees:OANDA',
    })
    testing.main(importer)
