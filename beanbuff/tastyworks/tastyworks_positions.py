"""Tastyworks - Parse positions CSV file.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Optional, Tuple
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords
from beanbuff.data import match
from beanbuff.tastyworks import tastysyms
from beanbuff.tastyworks.tastyutils import ToDecimal


def NormalizeAccountName(account: str) -> str:
    """Normalize to match that from the transactions log."""
    return "x{}".format(account[-4:]) if len(account) == 8 else account


_INSTYPES = {
    # 'EQUITY': 'Equity', ?
    'OPTION': 'Equity Option',
    'FUTURES': 'Future',
    'FUTURES_OPTION': 'Future Option',
}


def ConvertPoP(pop_str: str) -> Decimal:
    """Convert POP to an integer."""
    if pop_str == '< 1%':
        return Decimal(1)
    else:
        return Decimal(pop_str.rstrip('%'))


def GetPositions(filename: str) -> Tuple[Table, Table]:
    """Process the filename, normalize, and produce tables."""
    table = petl.fromcsv(filename)
    table = (table

             # Clean up account name to match that from the transactions log.
             .convert('Account', NormalizeAccountName)
             .rename('Account', 'account')

             # Make instrument type match that from the transactiosn log.
             .convert('Type', _INSTYPES)
             .rename('Type', 'instype')

             # Parse symbol and add instrument fields.
             .addfield('instrument', lambda r: tastysyms.ParseSymbol(
                 r['Symbol'], r['instype']))
             .cutout('Symbol')
             .addfield('symbol', lambda r: str(r.instrument), index=2)
             # TODO(blais): Cross-check these fields against the symbol, just to be sure.
             .cutout('Exp Date', 'DTE', 'Strike Price', 'Call/Put')
             .cutout('instrument')

             # Convert fields to Decimal values.
             .convert(['Trade Price',
                       'Cost',
                       'Mark',
                       'Net Liq',
                       'P/L Open',
                       'P/L Day',
                       'β Delta',
                       '/ Delta',
                       'Delta',
                       'Theta',
                       'Vega',
                       'IV Rank'], ToDecimal)

             # Convert POP to a fraction.
             .convert('PoP', ConvertPoP)

             # Rename some fields for normalization.
             .rename('Quantity', 'quantity')
             .rename('Trade Price', 'price')
             .rename('Cost', 'cost')
             .rename('Mark', 'mark')
             .rename('Net Liq', 'net_liq')
             .rename('P/L Open', 'pnl')
             .rename('P/L Day', 'pnl_day')


             #.addfield('DELTA_DIFFS', lambda r: r['Delta'] / r['/ Delta'] if r['/ Delta'] else '')
             .cut('account', 'instype', 'symbol',
                  'quantity', 'price', 'mark',
                  'cost', 'net_liq',
                  'pnl', 'pnl_day')
             )

    return table



# Regexp for matching filenames.
_FILENAME_RE = r"tastyworks_positions_(.*)_(\d{4}-\d{2}-\d{2}).csv"


def FindLatestPositionsFile(dirname: str) -> Optional[str]:
    """Find the latest transactions file in the directory."""
    found_pairs = []
    for fn in os.listdir(dirname):
        match = re.match(_FILENAME_RE, fn)
        if match:
            date_str = match.group(1)
            found_pairs.append((date_str, path.join(dirname, fn)))
    return sorted(found_pairs)[-1][1] if found_pairs else None


def IsPositionsFile(filename: str) -> bool:
    """Return true if this file is a matching transactions file."""
    return bool(re.match(_FILENAME_RE, path.basename(filename)))


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Main program."""
    table = GetPositions(filename)
    if 1:
        print(table.lookallstr())
        return


# TODO(blais): Render % of targets
# TODO(blais): Create breakdowns by expiration cycle.


if __name__ == '__main__':
    main()
