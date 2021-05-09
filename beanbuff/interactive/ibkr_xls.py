"""Extractor for XLS files from Interactive Brokers.

"""

import collections
import datetime
from pprint import pprint

from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core import data
from beancount.core import flags
from beancount.core import amount
from beancount.core import position

from beangulp import testing
from beangulp.importers.mixins import config
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier

from beanglers.mssb import xls_utils


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'asset_cash'         : 'Cash account',
        'transfer'           : 'Other account for inter-bank transfers',
    }

    SHEET_NAME = r'Deposit'

    # Number of days back to render.
    DAYS_BACK = 180

    matchers = [('mime', 'application/vnd.ms-excel')]

    def identify(self, file):
        if not super().identify(file):
            return False
        # Check if the spreadsheet has the sheet name we're looking for.
        return xls_utils.open_sheet(file.name, self.SHEET_NAME) != None

    def extract(self, file):
        sheet = xls_utils.open_sheet(file.name, self.SHEET_NAME)
        header, rows = xls_utils.extract_table(sheet)

        entries = []
        for index, row in enumerate(rows):
            meta = data.new_metadata(file.name, index)
            if row.method == 'ACH':  # Process deposit.
                entry = process_deposit(row, meta, self.config)
            else:
                raise ValueError("Unknown row type: {}".format(row))
            entries.append(entry)
            
        return entries


def process_deposit(row, meta, config):
    # Row(request_date='2018-12-03',
    #     reference_number='C17641554',
    #     method='ACH',
    #     account_id='U2738397',
    #     account_title='MARTIN BLAIS',
    #     delivering_institution='',
    #     from_account_number='',
    #     routing_number='',
    #     date_received='2018-12-03',
    #     date_available_for_trading='2018-12-07',
    #     date_available_for_withdrawal_original_bank='2018-12-07',
    #     date_available_for_withdrawal_other_bank='2019-02-06',
    #     amount='USD 50,000.00',
    #     status='Available')

    date = parse_date(row.request_date)
    narration = "Transfer ({})".format(row.method)
    tags = data.EMPTY_SET
    links = {"ibkr-{}".format(row.reference_number)}
    entry = data.Transaction(
        meta, date, flags.FLAG_OKAY, None, narration, tags, links, [])

    currency, number = row.amount.split(' ')
    amt = amount.Amount(D(number), currency)
    entry.postings.extend([
        data.Posting(config['transfer'], -amt, None, None, None, None),
        data.Posting(config['asset_cash'], amt, None, None, None, None)])

    return entry


def parse_date(string):
    "Parse a date string format."
    return datetime.datetime.strptime(string, '%Y-%m-%d').date()


if __name__ == '__main__':
    importer = Importer(filing="Assets:US:IBKR:Main", config={
        'asset_cash'         : "Assets:US:IBKR:Main:Cash",
        'transfer'           : "Assets:US:TD:Checking",
    })
    testing.main(importer)
