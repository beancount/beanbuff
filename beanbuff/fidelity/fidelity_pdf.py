"""Fidelity PDF statement importer.
"""

import re
import datetime
from os import path
from typing import Optional

from beancount.core import data

from beanbuff.utils import pdf
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


convert_to_text = pdf.convert_pdf_to_text


class Importer(beangulp.Importer):

    def __init__(self, filing: str):
        self._account = filing

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        if utils.is_mimetype(filepath, 'application/pdf'):
            contents = convert_to_text(filepath)
            return re.search('Fidelity Brokerage Services', contents)

    def filename(self, filepath: str) -> Optional[str]:
        return 'fidelity.{}'.format(path.basename(filepath))


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Fidelity:Main')
    testing.main(importer)
