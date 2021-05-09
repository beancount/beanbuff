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

from johnny.base.numbers import ToDecimal
from johnny.base.etl import petl, Table, Record, WrapRecords
from beanbuff.data import match
from beanbuff.tastyworks import tastysyms


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
    elif pop_str == '> 99.5%':
        return Decimal(99.5)
    else:
        return Decimal(pop_str.rstrip('%'))


def GetPositions(filename: str) -> Table:
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
             .rename('P/L Open', 'pnl_open')
             .rename('P/L Day', 'pnl_day')

             # Add a group field, though there aren't any groupings yet.
             .addfield('group', None)

             #.addfield('DELTA_DIFFS', lambda r: r['Delta'] / r['/ Delta'] if r['/ Delta'] else '')
             # 'instype'
             .cut('account', 'group', 'symbol',
                  'quantity', 'price', 'mark',
                  'cost', 'net_liq',
                  'pnl_open', 'pnl_day')
             )

    return table


def MatchFile(filename: str) -> Optional[Tuple[str, str, callable]]:
    """Return true if this file is a matching positions file."""
    _FILENAME_RE = r"tastyworks_positions_(.*)_(\d{4}-\d{2}-\d{2}).csv"
    match = re.match(_FILENAME_RE, path.basename(filename))
    if not match:
        return None
    account, date = match.groups()
    return account, date, GetPositions


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Main program."""
    table = GetPositions(filename)
    print(table.lookallstr())


# TODO(blais): Render % of targets
# TODO(blais): Create breakdowns by expiration cycle.


if __name__ == '__main__':
    main()
