"""LendingClub PDF statement importer.
"""
import re

from dateutil.parser import parse as parse_datetime

from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier
from beangulp import testing

from beanglers.tools import pdfconvert


class Importer(identifier.IdentifyMixin, filing.FilingMixin):

    matchers = [('mime', 'application/pdf'),
                ('content', 'LendingClub')]

    converter = staticmethod(pdfconvert.convert_to_text)

    def file_date(self, file):
        """Try to get the date of the report from the filename."""
        filename = file.name

        text = file.convert(self.converter)
        match = re.search(r"(.* 20\d\d) - (.* 20\d\d)", text)
        if match:
            return parse_datetime(match.group(2)).date()

        match = re.search(r"([A-Za-z]+) \d\d-(\d\d)\. (20\d\d)", text)
        if match:
            return parse_datetime(' '.join(match.group(1,2,3))).date()


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:LendingClub')
    testing.main(importer)
