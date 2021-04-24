"""Parse and normalize Tastyworks transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
"""

import collections
from decimal import Decimal
from os import path
from typing import Optional, Tuple
import datetime
import hashlib
import logging
import pprint
import re

import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords
from beanbuff.data import match
from beanbuff.tastyworks import tastysyms


debug = False
ZERO = Decimal(0)


def ToDecimal(value: str):
    """Convert number to decimal."""
    return Decimal(value.replace(',', '')) if value else ZERO


def GetTransactionId(rec: Record) -> str:
    """Make up a unique transaction id."""
    md5 = hashlib.blake2s(digest_size=6)
    md5.update(rec['Order #'].encode('ascii'))
    md5.update(rec['Description'].encode('ascii'))
    return "^{}".format(md5.hexdigest())


def GetRowType(rowtype: str) -> str:
    """Validate the row type."""
    assert rowtype in {'Trade', 'Expiration', 'Mark'}
    return rowtype


def GetPrice(rec: Record) -> Decimal:
    """Get the per-contract price."""
    match = re.search(r"@ ([0-9.]+)$", rec.Description)
    if not match:
        raise ValueError("Could not infer price from description: {}".format(rec))
    return Decimal(match.group(1))


def GetMultiplier(rec: Record) -> Decimal:
    """Get the underlying contract multiplier."""
    multiplier = rec.instrument.multiplier
    if rec['Instrument Type'] != 'Future':
        # Sanity check: Verify that the approximate multiplier you can compute
        # using the (rounded) average price is close to the one we infer from
        # our futures library. This is a cross-check for the futures library
        # code.
        approx_multiplier = abs(rec['Average Price']) / rec.price
        assert 0.9995 < (multiplier / approx_multiplier) < 1.0005, (
            multiplier, rec['Average Price'], rec.price)
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
        raise NotImplementedError("Unknown instruction: {}".format(rec))


def GetPosEffect(rec: Record) -> Optional[str]:
    """Get position effect."""
    if rec.Action.endswith('TO_OPEN'):
        return 'OPENING'
    elif rec.Action.endswith('TO_CLOSE'):
        return 'CLOSING'
    else:
        return ''


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
                       'Fees',
                       'Strike Price'], ToDecimal)

             # Parse the instrument from the original row.
             .addfield('instrument', lambda r: tastysyms.ParseSymbol(
                 r['Symbol'], r['Instrument Type']))

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
             # represents the multiplier of the
             .cutout('Multiplier')

             # Process, clean up and validate the strike price.
             .addfield('strike', GetStrike)
             .cutout('Strike Price')

             # Add expiration code.
             .addfield('expcode', lambda r: r.instrument.expcode)

             # Rename some of the columns to be passed through.
             .rename('Order #', 'order_id')
             .rename('Instrument Type', 'instype')
             .rename('Description', 'description')
             .rename('Call or Put', 'side')
             .rename('Quantity', 'quantity')
             .rename('Value', 'cost')
             .rename('Commissions', 'commissions')
             .rename('Fees', 'fees')

             # Split 'Action' field.
             .addfield('instruction', GetInstruction)
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
             .cut('account', 'transaction_id', 'datetime', 'rowtype', 'order_id',
                  'instype', 'underlying', 'expiration', 'expcode',
                  'side', 'strike', 'multiplier',
                  'effect', 'instruction', 'quantity', 'price',
                  'cost', 'commissions', 'fees',
                  'description')
             )

    return table.sort('datetime')


def SplitTables(table: Table) -> Tuple[Table, Table]:
    """Split the table into transactions and others."""
    return table.biselect(lambda r: r.Type == 'Trade')


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


def AddOrderTotals(table: Table) -> Table:
    """Add totals per order."""

    credits_map = {rec.order_id: rec.value
                   for rec in (table
                               .aggregate('order_id', sum, 'cost')
                               .records())}

    def AddCredit(prv, cur, nxt) -> Decimal:
        if nxt is None or nxt.order_id != cur.order_id:
            return credits_map.get(cur.order_id, None)
        return ''

    def AddBalance(prv, cur, nxt) -> Decimal:
        return (ZERO if prv is None else prv._balance) + cur.cost

    def CleanBalance(prv, cur, nxt) -> Decimal:
        return (cur._balance
                if nxt is None or nxt.order_id != cur.order_id
                else '')

    return (table
            .addfieldusingcontext('credit', AddCredit)
            .addfieldusingcontext('_balance', AddBalance)
            .addfieldusingcontext('balance', CleanBalance)
            .cutout('_balance'))


def CalculatePositionCost(table: Table) -> Decimal:
    """Calculate the original cost to acquire an active position."""

    invs = collections.defaultdict(match.FifoInventory)
    for rec in table.records():
        instrument_key = (rec.underlying, rec.expiration, rec.expcode, rec.side, rec.strike)
        inv = invs[instrument_key]
        sign = 1 if rec.instruction == 'BUY' else -1
        inv.match(sign * rec.quantity, rec.cost / rec.quantity, rec.match_id)
        #print("    ", rec)

    #print()
    active = False
    total_basis = ZERO
    for key, inv in invs.items():
        position, basis, match_id = inv.position()
        total_basis += basis
        if position:
            print(key, position, basis, match_id)
            active = True
    if active:
        print('DONE', total_basis)
        print()


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Main program."""
    trades_table, _ = GetTransactions(filename)
    from beanbuff.data import match
    trades_table = match.Match(trades_table)
    from beanbuff.data import chains
    trades_table = chains.Group(trades_table)
    if 0:
        print(trades_table.lookallstr())


    # Group by chain and render.
    chain_map = trades_table.recordlookup('chain_id')
    for chain_id, rows in chain_map.items():
        rows = list(rows)
        chain_table = AddOrderTotals(WrapRecords(list(rows)))

        print("* {}.{:%y%m%d}-{:%y%m%d} ({})".format(
            rows[0].underlying,
            rows[0].datetime,
            rows[-1].datetime,
            chain_id))

        print(chain_table.lookallstr())
        CalculatePositionCost(chain_table)

        continue

        last_order_id = None
        cost = ZERO
        for row in rows:
            if row.order_id != last_order_id:
                print()
                last_order_id = row.order_id
            print("    {}".format(row.description))
        print()
        print()


if __name__ == '__main__':
    main()
