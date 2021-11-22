"""Extractor for XLS files from Interactive Brokers.

TODO(blais): Convert this to using the completed importer in Johnny.
"""

import collections
import datetime
from pprint import pprint
from os import path
from typing import Dict, Optional

from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core import data
from beancount.core import flags
from beancount.core import amount
from beancount.core import position

from beanglers.mssb import xls_utils  # TODO(blais): Move to public.
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


CONFIG = {
    'asset_cash'         : 'Cash account',
    'transfer'           : 'Other account for inter-bank transfers',
}


def extract(filepath: str, config: Dict[str, str]) -> data.Entries:
    sheet = xls_utils.open_sheet(filepath, SHEET_NAME)
    header, rows = xls_utils.extract_table(sheet)
    entries = []
    for index, row in enumerate(rows):
        meta = data.new_metadata(filepath, index)
        if row.method == 'ACH':  # Process deposit.
            entry = process_deposit(row, meta, config)
        else:
            raise ValueError("Unknown row type: {}".format(row))
        entries.append(entry)
    return entries


class Importer(beangulp.Importer):

    def __init__(self, filing, config):
        self._account = filing
        self.config = config

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        return (utils.is_mimetype(filepath, 'application/vnd.ms-excel') and
                # Check if the spreadsheet has the sheet name we're looking for.
                xls_utils.open_sheet(filepath, SHEET_NAME) != None)

    def date(self, filepath: str) -> Optional[datetime.date]:
        pass # TODO(blais):

    def filename(self, filepath: str) -> Optional[str]:
        return 'ibkr.{}'.format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        return extract(filepath, self.config)


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
