"""RBC Checking, Savings and Credit Card PDF statements importer.
"""
import re

import dateutil.parser

from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier
from beangulp import testing

from beanglers.tools import pdfconvert


class Importer(identifier.IdentifyMixin, filing.FilingMixin):

    matchers = [('mime', 'application/pdf'),
                ('content', 'OANDA Corporation')]

    converter = staticmethod(pdfconvert.convert_to_text)

    def file_date(self, file):
        filename = file.name
        text = file.convert(self.converter)
        match = re.search(r'Statement Period.*'
                          r'Account Number.*'
                          r'([A-Z][a-z][a-z] \d\d) - ([A-Z][a-z][a-z] \d\d), (\d\d\d\d)',
                          text, re.DOTALL)
        assert match, "Expected date not found in file."
        return dateutil.parser.parse('{} {}'.format(*match.group(2, 3))).date()


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:OANDA:Hedging')
    testing.main(importer)
