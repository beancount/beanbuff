#!/usr/bin/env python3
"""Translate all the Ameritrade symbols to normalized symbology.

- Equities options: %m%d%y is replaced by %y%m%d in equities options,
  e.g. 'TLT_041621P131' becomes 'TLT_210416P131'

- Futures options are modified to include their expiration dates:
  '/NQH21_QNEG21C13100' becomes '/NQH21_QNEG21_210221C13100'

This reads my Beancount file and converts the symbols to the new standardized
symbology I use. See http://furius.ca/beancount/doc/symbology for details.
"""

import argparse
import logging
import re

from beancount import loader
from beancount.core import data
from beancount.parser import printer


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument('filename', help='Ledger filename')
    args = parser.parse_args()

    entries, _, __ = loader.load_file(args.filename)

    new_entries = []
    for entry in entries:
        if isinstance(entry, data.Transaction):
            translated = False
            new_postings = []
            for posting in entry.postings:
                if not re.search('Ameritrade', posting.account):
                    new_postings.append(posting)
                    continue
                translated = True
                units, cost, price = posting.units, posting.cost, posting.price
                units = units._replace(currency=translate(units.currency))
                if cost is not None and cost.currency is not None:
                    cost = cost._replace(currency=translate(cost.currency))
                if price is not None and price.currency is not None:
                    price = price._replace(currency=translate(price.currency))
                new_postings.append(
                    posting._replace(units=units, cost=cost, price=price))
            entry = entry._replace(postings=new_postings)
            if translated:
                printer.print_entry(entry)

        new_entries.append(entry)



def translate(currency: data.Currency):
    match = re.match("([A-Z]+)_(\d{6})([CP]\d+)", currency)
    if not match:
        return currency
    date = match.group(2)
    new_date = date[4:6] + date[0:4]
    return "{}_{}{}".format(match.group(1), new_date, match.group(3))


if __name__ == '__main__':
    main()
