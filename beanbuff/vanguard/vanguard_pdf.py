"""Vanguard PDF statement importer.
"""

from os import path
from typing import Optional
import datetime
import re

import dateutil.parser

from beancount.core import data

from beanbuff.utils import pdf
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


convert_to_text = pdf.convert_pdf_to_text


def get_date(text: str) -> datetime.date:
    match = re.search(
        r"ACCOUNT SUMMARY: (\d\d/\d\d/\d\d\d\d) - (\d\d/\d\d/\d\d\d\d)", text
    )
    assert match, "Expected date not found in file."
    return dateutil.parser.parse(match.group(2)).date()


class Importer(beangulp.Importer):
    def __init__(self, filing: str):
        self._account = filing

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        if utils.is_mimetype(filepath, "application/pdf"):
            contents = convert_to_text(filepath)
            return all(
                re.search(x, contents)
                for x in ["Vanguard", "vanguard.com", r"INC. 401\(K\) SAVINGS PLAN"]
            )

    def date(self, filepath: str) -> Optional[datetime.date]:
        contents = convert_to_text(filepath)
        return get_date(contents)

    def filename(self, filepath: str) -> Optional[str]:
        return "vanguard.{}".format(path.basename(filepath))


if __name__ == "__main__":
    importer = Importer(filing="Assets:US:Vanguard:Cash")
    testing.main(importer)
