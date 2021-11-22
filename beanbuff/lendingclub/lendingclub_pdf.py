"""LendingClub PDF statement importer.
"""
import re
import datetime
from typing import Optional
from os import path

from dateutil.parser import parse as parse_datetime

from beancount.core import data

from beanbuff.utils import pdf
from beangulp import testing
from beangulp import utils
import beangulp


convert_to_text = pdf.convert_pdf_to_text


def get_date(text: str) -> datetime.date:
    match = re.search(r"(.* 20\d\d) - (.* 20\d\d)", text)
    if match:
        return parse_datetime(match.group(2)).date()

    match = re.search(r"([A-Za-z]+) \d\d-(\d\d)\. (20\d\d)", text)
    if match:
        return parse_datetime(' '.join(match.group(1,2,3))).date()


class Importer(beangulp.Importer):

    def __init__(self, filing: str, account_id: str):
        self._account = filing
        self.account_id = account_id

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        if utils.is_mimetype(filepath, 'application/pdf'):
            contents = convert_to_text(filepath)
            if re.search('LendingClub', contents):
               return bool(self.account_id and
                           re.search(f'ACCOUNT #{self.account_id}', contents))

    def date(self, filepath: str) -> Optional[datetime.date]:
        contents = convert_to_text(filepath)
        return get_date(contents)

    def filename(self, filepath: str) -> Optional[str]:
        return 'lendingclub.{}'.format(path.basename(filepath))


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:LendingClub', account_id='6\d+')
    testing.main(importer)
