"""Tastyworks - Parse transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
"""

import collections
import decimal
from decimal import Decimal
from os import path
from typing import Any, List, Optional, Tuple
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
from beanbuff.data import transactions as txnlib


ZERO = Decimal(0)
ONE = Decimal(1)


def GetTransactionId(rec: Record) -> str:
    """Make up a unique transaction id."""
    md5 = hashlib.blake2s(digest_size=6)
    md5.update(rec['Order #'].encode('ascii'))
    md5.update(rec['Description'].encode('ascii'))
    return "^{}".format(md5.hexdigest())


_ROW_TYPES = {
    'Trade': 'Trade',
    'Receive Deliver': 'Expire',
}

def GetRowType(rowtype: str) -> str:
    """Validate the row type."""
    try:
        return _ROW_TYPES[rowtype]
    except KeyError:
        return KeyError("Invalid rowtype: '{}'".format(rowtype))


def GetPrice(rec: Record) -> Decimal:
    """Get the per-contract price."""
    if rec.rowtype == 'Expire':
        return ZERO
    match = re.search(r"@ ([0-9.]+)$", rec.Description)
    if not match:
        raise ValueError("Could not infer price from description: {}".format(rec))
    return Decimal(match.group(1))


def GetMultiplier(rec: Record) -> Decimal:
    """Get the underlying contract multiplier."""

    # Use the multiplier from the instrument.
    multiplier = rec.instrument.multiplier

    # Check the multiplier for stocks (which is normally unset).
    if rec['Instrument Type'] == 'Equity':
        assert multiplier == 1

    # Sanity check: Verify that the approximate multiplier you can compute using
    # the (rounded) average price is close to the one we infer from our futures
    # library. This is a cross-check for the futures library code.
    if rec['Instrument Type'] != 'Future' and rec['Average Price'] != ZERO:
        approx_multiplier = abs(rec['Average Price']) / rec.price
        assert 0.9995 < (multiplier / approx_multiplier) < 1.0005, (
            multiplier, rec['Average Price'], rec.price)
    assert isinstance(multiplier, int)
    return multiplier


def GetExpiration(expi_str: str) -> Optional[datetime.date]:
    """Get the contract expiration date."""
    return (datetime.datetime.strptime(expi_str, "%m/%d/%y").date()
            if expi_str
            else None)


def GetStrike(rec: Record) -> Optional[Decimal]:
    """Process, clean up and validate the strike price."""
    strike = rec['Strike Price']
    if strike:
        assert rec.instrument.strike == strike, (
            rec.instrument.strike, strike)
        return strike
    return None


def GetInstruction(rec: Record) -> Optional[str]:
    """Get instruction."""
    if rec.Action.startswith('BUY'):
        return 'BUY'
    elif rec.Action.startswith('SELL'):
        return 'SELL'
    else:
        raise NotImplementedError("Unknown instruction: '{}'".format(rec.Action))


def GetPosEffect(rec: Record) -> Optional[str]:
    """Get position effect."""
    if rec.Action.endswith('TO_OPEN'):
        return 'OPENING'
    elif rec.Action.endswith('TO_CLOSE'):
        return 'CLOSING'
    elif rec.rowtype == 'Expire':
        return 'CLOSING'
    else:
        return '?'


def ParseStrikePrice(string: str) -> Decimal:
    """Parse and normalize the strike price."""
    cstring = re.sub(r"(.*)\.0$", r"\1", string)
    if not cstring:
        return Decimal(0)
    try:
        return Decimal(cstring)
    except decimal.InvalidOperation:
        raise ValueError("Could not parse: {}".format(string))


def NormalizeTrades(table: petl.Table, account: str) -> petl.Table:
    """Prepare the table for processing."""

    table = (table

             # WARNING: Don't use 'Average Price' for anything serious, it is a
             # rounded value.

             # Synthesize a unique transaction id field, since none is provided.
             .addfield('transaction_id', GetTransactionId)

             # Convert fields to Decimal values.
             .convert(['Value',
                       'Average Price',
                       'Quantity',
                       'Multiplier',
                       'Commissions',
                       'Fees'], ToDecimal)
             .convert('Strike Price', ParseStrikePrice)

             # Parse the instrument from the original row.
             .addfield('instrument', lambda r: tastysyms.ParseSymbol(
                 r['Symbol'], r['Instrument Type']))
             .addfield('symbol', lambda r: str(r.instrument))

             # Add underlying with the normalized futures contract month code.
             .addfield('underlying', lambda r: r.instrument.dated_underlying)

             # Add the account id.
             .addfield('account', account)

             # Normalize the type.
             .rename('Type', 'rowtype')
             .convert('rowtype', GetRowType)

             # Parse the date into datetime.
             .convert('Date', parser.parse)
             .addfield('datetime', lambda d: d.Date.replace(tzinfo=None))
             .cutout('Date')

             # Convert the futures expiration date.
             .convert('Expiration Date', GetExpiration)
             .rename('Expiration Date', 'expiration')

             # Infer the per-contract price.
             .addfield('price', GetPrice)

             # Infer the per-contract multiplier.
             .addfield('multiplier', GetMultiplier)

             # We remove the original multiplier column because it only
             # represents the multiplier of the average price and is innacurate.
             # We want the multiplier of the quantity.
             .cutout('Multiplier')

             # Process, clean up and validate the strike price.
             .addfield('strike', GetStrike)
             .cutout('Strike Price')

             # Add expiration code.
             .addfield('expcode', lambda r: r.instrument.expcode)

             # Rename some of the columns to be passed through.
             .rename('Order #', 'order_id')
             .convert('order_id', lambda v: v or None)
             .rename('Instrument Type', 'instype')
             .rename('Description', 'description')
             .rename('Call or Put', 'putcall')
             .rename('Quantity', 'quantity')
             .rename('Value', 'cost')
             .rename('Commissions', 'commissions')
             .rename('Fees', 'fees')

             # Split 'Action' field.
             .addfield('instruction',
                       lambda r: GetInstruction(r) if r.rowtype == 'Trade' else '')
             .addfield('effect', GetPosEffect)

             # Remove instrument we parsed early on.
             .cutout('instrument')

             # Removed remaining unnecessary columns.
             .cutout('Symbol')
             .cutout('Underlying Symbol')
             .cutout('Average Price')
             .cutout('Action')

             # # Sort by date incremental.
             # .sort('Date')

             # See transactions.md.
             .cut(txnlib.FIELDS)
             )

    return table.sort('datetime')


def SplitTables(table: Table) -> Tuple[Table, Table]:
    """Split the table into transactions and others."""
    return table.biselect(lambda r: r.Type != 'Money Movement')


def GetAccount(filename: str) -> str:
    """Get the account id."""
    match = re.match(r'tastyworks_transactions_(.*)_'
                     r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}).csv',
                     path.basename(filename))
    if not match:
        logging.error("Could not figure out the account name from the filename")
        account = None
    else:
        account = match.group(1)
    return account


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Process the filename, normalize, and produce tables."""
    table = petl.fromcsv(filename)
    trades_table, other_table = SplitTables(table)
    norm_trades_table = NormalizeTrades(trades_table, GetAccount(filename))
    return norm_trades_table, other_table


def MatchFile(filename: str) -> Optional[Tuple[str, str, callable]]:
    """Return true if this file is a matching transactions file."""
    _FILENAME_RE = (r"tastyworks_transactions_(.*)_"
                    r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}).csv")
    match = re.match(_FILENAME_RE, path.basename(filename))
    if not match:
        return None
    account, date1, date2 = match.groups()
    return account, date2, txnlib.MakeParser(GetTransactions)


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Main program."""
    trades_table, _ = GetTransactions(filename)
    if 1:
        print(trades_table.lookallstr())
        return


if __name__ == '__main__':
    main()
