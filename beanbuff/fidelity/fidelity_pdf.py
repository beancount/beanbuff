"""Fidelity PDF statement importer.
"""

from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier
from beangulp import testing

from beanglers.tools import pdfconvert


class Importer(identifier.IdentifyMixin, filing.FilingMixin):

    matchers = [('mime', 'application/pdf'),
                ('content', 'Fidelity Brokerage Services')]

    converter = staticmethod(pdfconvert.convert_to_text)


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Fidelity:Main')
    testing.main(importer)
