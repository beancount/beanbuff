"""Think-or-Swim platform transaction detail importer.

This code parses the file that can be downloaded from the Think-or-Swim
application from the Activity page.
"""
import csv
import re
import itertools
import datetime
import collections
from typing import Optional
from os import path

from beancount.core import data
from beancount.core import flags
from beancount.core.number import D, ZERO

from beangulp import csv_utils
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


debug = False


def convert_number(string):
    if not string or string == '--':
        return D()
    mo = re.match(r'\((.*)\)', string)
    if mo:
        sign = -1
        string = mo.group(1)
    else:
        sign = 1

    number = D(re.sub('[\$,]', '', string)) if string != '--' else D()
    return number * sign


CONFIG = {
    'cash_currency'      : 'Currency used for cash account',
    'asset_cash'         : 'Cash account',
    #'asset_position'     : 'Root account for all position sub-accounts',
    'fees'               : 'Fees',
    'commission'         : 'Commissions',
    'interest'           : 'Interest income',
    'pnl'                : 'Capital Gains/Losses',
    'transfer'           : 'Other account for inter-bank transfers',
}


class Importer(beangulp.Importer):

    def __init__(self, filing, config):
        self._account = filing
        self.config = config
        utils.validate_accounts(CONFIG, config)

    def identify(self, filepath: str) -> bool:
        return (utils.is_mimetype(filepath, 'text/csv') and
                utils.search_file_regexp(filepath, '', nbytes=4096))

    def account(self, filepath: str) -> data.Account:
        return self._account

    def filename(self, filepath: str) -> Optional[str]:
        return 'thinkorswim_forex.{}'.format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        return extract(self.config, filepath)


def extract(config, filename):
    """Import a CSV file from Think-or-Swim."""
    with open(filename) as infile:
        sections = csv_utils.csv_split_sections_with_titles(csv.reader(infile))
    if 0:
        for section_name, rows in sections.items():
            if re.search(r'\bSummary\b', section_name):
                continue
            print('============================================================', section_name)
            if not rows:
                continue
            irows = iter(rows)
            fieldnames = csv_utils.csv_clean_header(next(irows))
            Tuple = collections.namedtuple('Row', fieldnames)
            for row in irows:
                obj = Tuple(*row)
                print(obj)

    return process_forex(sections['Forex Statements'], filename, config,
                         flag=flags.FLAG_OKAY)


def process_forex(section, filename, config, flag='*'):
    """Process the FOREX subaccount entries."""
    new_entries = []
    cash_currency = config['cash_currency']

    irows = iter(section)
    prev_balance = D()
    running_balance = D()
    prev_date = datetime.date(1970, 1, 1)
    fieldnames = csv_utils.csv_clean_header(next(irows))
    Tuple = collections.namedtuple('Row', fieldnames)
    for index, row in enumerate(itertools.starmap(Tuple, irows)):

        # For transfers, they don't put the date in (WTF?) so use the previous
        # day's date, because the rows are otherwise correctly sorted. Then
        # parse the date.
        if not row.date:
            date = prev_date
        else:
            date = datetime.datetime.strptime(row.date, '%d/%m/%y').date()

        balance = convert_number(row.balance)

        row_amount = convert_number(row.amount_usd)
        running_balance += row_amount

        ##print('RUNNING_BALANCE', running_balance, balance)
        ##assert(abs(running_balance - balance) <= D('0.01'))

        amount = balance - prev_balance
        ##assert(abs(row_amount - amount) <= D('0.01'))

        # Check some invariants.
        ##assert row.commissions_fees == '--'

        if row.type not in ('BAL',):

            # Create a new transaction.
            narration = re.sub('[ ]+', ' ', "({0.type}) {0.description}".format(row).replace('\n', ' ')).strip()
            fileloc = data.new_metadata(filename, index)
            links = set([row.ref] if row.ref != '--' else [])
            entry = data.Transaction(fileloc, date, flag, None, narration, data.EMPTY_SET, links, [])

            if row.type in ('FND', 'WDR'):
                data.create_simple_posting(entry, config['transfer'], amount, cash_currency)
                data.create_simple_posting(entry, config['asset_cash'], -amount, cash_currency)

            elif row.type == 'TRD':
                data.create_simple_posting(entry, config['asset_cash'], amount, cash_currency)
                data.create_simple_posting(entry, config['pnl'], -amount, cash_currency)

            elif row.type == 'ROLL':
                if amount != ZERO:
                    data.create_simple_posting(entry, config['asset_cash'], amount, cash_currency)
                    data.create_simple_posting(entry, config['pnl'], -amount, cash_currency)

            if entry.postings:
                new_entries.append(entry)

        prev_date = date
        prev_balance = balance

    return new_entries


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Ameritrade:Forex', config={
        'cash_currency'      : 'USD',
        'asset_cash'         : 'Assets:US:Ameritrade:Forex',
        #'asset_position'     : 'Assets:US:Ameritrade:Forex',
        'fees'               : 'Expenses:Financial:Fees',
        'commission'         : 'Expenses:Financial:Commissions',
        'interest'           : 'Income:US:Ameritrade:Forex:Interest',
        'pnl'                : 'Income:US:Ameritrade:Forex:PnL',
        'transfer'           : 'Assets:US:Ameritrade:Main:Cash',
    })
    testing.main(importer)
