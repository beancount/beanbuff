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
import collections
import logging
import re
from typing import Dict

from beancount import loader
from beancount.core import data
from beancount.parser import printer


def Translate(currency: data.Currency):
    match = re.match("([A-Z]+)_(\d{6})([CP][0-9.]+)", currency)
    if match:
        date = match.group(2)
        new_date = date[4:6] + date[0:4]
        return "{}_{}_{}".format(match.group(1), new_date, match.group(3))

    match = re.match("(/?[A-Z0-9]+)_([A-Z][A-Z0-9]+)([CP][0-9.]+)", currency)
    if match:
        date = match.group(2)
        new_date = date[:-1] + '2' + date[-1:]
        return "{}_{}_{}".format(match.group(1), new_date, match.group(3))

    match = re.fullmatch("(/?[A-Z0-9]+)([FGHJKMNQUVXZ])(\d)", currency)
    if match:
        return "{}{}2{}".format(match.group(1), match.group(2), match.group(3))

    return None


def GetTranslationMap(entries) -> Dict[str, str]:
    """Create a mapping of (old, new) current names."""

    mapping = collections.defaultdict(set)
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
                mapping[units.currency] = Translate(units.currency)

                if cost is not None and cost.currency is not None:
                    mapping[cost.currency] = Translate(cost.currency)

                if price is not None and price.currency is not None:
                    mapping[price.currency] = Translate(price.currency)

            entry = entry._replace(postings=new_postings)

        elif isinstance(entry, data.Balance):
            mapping[entry.amount.currency] = Translate(entry.amount.currency)

        elif isinstance(entry, data.Commodity):
            mapping[entry.currency] = Translate(entry.currency)

        elif isinstance(entry, data.Price):
            mapping[entry.currency] = Translate(entry.currency)
            mapping[entry.amount.currency] = Translate(entry.amount.currency)

    return {key: name for key, name in mapping.items() if name is not None}


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument('filename', help='Ledger filename')
    parser.add_argument('process_files', nargs='+', help='Filenames to process')
    args = parser.parse_args()

    entries, _, __ = loader.load_file(args.filename)
    translation_map = GetTranslationMap(entries)
    if 0:
        #print(Translate('LIT_210416_C67'))
        pp(translation_map)

    else:
        regexp_str = r'({})'.format('|'.join(list(translation_map.keys())))
        regexp = re.compile(regexp_str)
        for filename in args.process_files:
            with open(filename) as replfile:
                new_contents = regexp.sub(
                    lambda mo: translation_map.get(mo.group(1), mo.group(1)),
                    replfile.read())
            with open(filename, 'w') as outfile:
                outfile.write(new_contents)


if __name__ == '__main__':
    main()
