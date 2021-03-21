"""Vanguard PDF statement importer.
"""
import re

import dateutil.parser

from beangulp import testing
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier

from beanbuff.utils import pdf


class Importer(identifier.IdentifyMixin, filing.FilingMixin):

    matchers = [
        ('mime', 'application/pdf'),
        ('content', 'Vanguard'),
        ('content', 'vanguard.com'),
        ('content', r'INC. 401\(K\) SAVINGS PLAN'),
    ]

    converter = staticmethod(pdf.convert_pdf_to_text)

    def file_date(self, file):
        filename = file.name
        text = file.convert(pdf.convert_pdf_to_text)
        match = re.search(r'ACCOUNT SUMMARY: (\d\d/\d\d/\d\d\d\d) - (\d\d/\d\d/\d\d\d\d)', text)
        assert match, "Expected date not found in file."
        return dateutil.parser.parse(match.group(2)).date()


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Vanguard:Cash')
    testing.main(importer)
