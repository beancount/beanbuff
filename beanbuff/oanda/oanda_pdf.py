"""OANDA PDF reports.
"""
import datetime
import re
from typing import Optional
from os import path

import dateutil.parser

from beancount.core import data

import beangulp
from beangulp import testing
from beanbuff.utils import pdf
from beangulp import petl_utils
from beangulp import utils


convert_to_text = pdf.convert_pdf_to_text


class Importer(beangulp.Importer):

    def __init__(self, filing: str, account_id: str):
        self._account = filing
        self.account_id = account_id

    def identify(self, filepath: str) -> bool:
        if utils.is_mimetype(filepath, 'application/pdf'):
            contents = convert_to_text(filepath)
            if re.search(r'OANDA Corporation', contents):
                return bool(re.search(rf'\b{self.account_id}\b', contents))

    def account(self, filepath: str) -> data.Account:
        return self._account

    def date(self, filepath: str) -> Optional[datetime.date]:
        contents = convert_to_text(filepath)
        return get_date(contents)

    def filename(self, filepath: str) -> Optional[str]:
        return 'oanda.{}'.format(path.basename(filepath))


def get_date(text: str) -> datetime.date:
        match = re.search(r'Statement Period.*'
                          r'Account Number.*'
                          r'([A-Z][a-z][a-z] \d\d) - ([A-Z][a-z][a-z] \d\d), (\d\d\d\d)',
                          text, re.DOTALL)
        assert match, "Expected date not found in file."
        return dateutil.parser.parse('{} {}'.format(*match.group(2, 3))).date()


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:OANDA:Hedging', account_id='9')
    testing.main(importer)
