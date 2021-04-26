"""Tastyworks - Parse transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
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

import click
from dateutil import parser

from beanbuff.data.etl import petl, Table, Record, WrapRecords
from beanbuff.data import match
from beanbuff.tastyworks import tastysyms

from beanbuff.tastyworks import tastyworks_transactions
from beanbuff.ameritrade import thinkorswim_transactions


ZERO = Decimal(0)
Q1 = Decimal('1')
Q = Decimal('0.01')



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


def GetChainAmounts(table: Table) -> Decimal:
    """Calculate the original cost to acquire an active position."""

    # We just sum up the MTM rows, which should still be at basis.
    trade_table, mark_table = table.biselect(lambda r: r.rowtype == 'Trade')

    active = mark_table.nrows() != 0
    basis = sum(mark_table.values('cost'))
    accr_cr = sum(trade_table.values('cost'))
    init_cr = next(iter(trade_table
                             .aggregate('order_id', sum, 'cost')
                             .head(1)
                             .values('value')))

    return active, -basis, init_cr, accr_cr


win_frac = Decimal('0.50')
p50 = Decimal('0.80')


def CalculateExitRow(basis: Decimal, init_cr: Decimal, accr_cr: Decimal) -> Any:
    """Calculate all the thresholds for exit."""

    if 0:
        init_win = init_cr * win_frac
        init_lose = -(init_win * p50 / (1 - p50)).quantize(Q)
        init_netliq_win = basis + init_win
        init_netliq_flat = basis
        init_netliq_lose = basis + init_lose

        init_pnl_win = accr_cr + init_netliq_win
        init_pnl_flat = accr_cr + init_netliq_flat
        init_pnl_lose = accr_cr + init_netliq_lose

        accr_win = accr_cr * win_frac
        accr_lose = -(accr_win * p50 / (1 - p50)).quantize(Q)
        accr_netliq_win = basis + accr_win
        accr_netliq_flat = basis
        accr_netliq_lose = basis + accr_lose

        accr_pnl_win = accr_cr + accr_netliq_win
        accr_pnl_flat = accr_cr + accr_netliq_flat
        accr_pnl_lose = accr_cr + accr_netliq_lose
    else:
        init_win = init_cr * win_frac
        init_lose = -(init_win * p50 / (1 - p50)).quantize(Q)
        accr_win = accr_cr * win_frac
        accr_lose = -(accr_win * p50 / (1 - p50)).quantize(Q)

        init_netliq_flat = -accr_cr
        init_netliq_win = init_netliq_flat + init_win
        init_netliq_lose = init_netliq_flat + init_lose

        init_pnl_flat = basis + init_netliq_flat
        init_pnl_win = init_pnl_flat + init_win
        init_pnl_lose = init_pnl_flat + init_lose

        accr_netliq_flat = -accr_cr
        accr_netliq_win = accr_netliq_flat + accr_win
        accr_netliq_lose = accr_netliq_flat + accr_lose

        accr_pnl_flat = basis + accr_netliq_flat
        accr_pnl_win = accr_pnl_flat + accr_win
        accr_pnl_lose = accr_pnl_flat + accr_lose

    return ((init_win, init_cr, init_lose,
             init_netliq_win, init_netliq_flat, init_netliq_lose,
             init_pnl_win, init_pnl_flat, init_pnl_lose),
            (accr_win, accr_cr, accr_lose,
             accr_netliq_win, accr_netliq_flat, accr_netliq_lose,
             accr_pnl_win, accr_pnl_flat, accr_pnl_lose))


# def RenderTrade(table: Table) -> str:
#     # Render the trade to something nicely readable.
#     #
#     # last_order_id = None
#     # cost = ZERO
#     # for row in rows:
#     #     if row.order_id != last_order_id:
#     #         print()
#     #         last_order_id = row.order_id
#     #     print("    {}".format(row.description))
#     # print()
#     # print()


@click.command()
@click.option('--tw', type=click.Path(resolve_path=True, exists=True))
@click.option('--td', type=click.Path(resolve_path=True, exists=True))
@click.option('-v', '--verbose', is_flag=True)
@click.option('--no-equity', is_flag=True)
def main(td: Optional[str], tw: Optional[str], verbose: bool, no_equity=True):
    """Main program."""

    # TODO(blais): Change the input so it's able to detect which file is which.

    # Read in the data files.
    tables = []
    if tw:
        trades_table, _ = tastyworks_transactions.GetTransactions(tw)
        tables.append(trades_table)
    if td:
        trades_table, _ = thinkorswim_transactions.GetTransactions(td)
        tables.append(trades_table)

    trades_table = petl.cat(*tables)
    if 0:
        print(trades_table.lookallstr())
        return

    # Remove equity if desired.
    #
    # TODO(blais): Handle this by subtracting existing transactions from the
    # Ledger instead.
    if no_equity:
        trades_table = trades_table.select(lambda r: r.instype != 'Equity')

    from beanbuff.data import match
    trades_table = match.Match(trades_table)
    from beanbuff.data import chains
    trades_table = chains.Group(trades_table)
    if 0:
        print(trades_table.lookallstr())

    # Group by chain and render.
    chain_map = trades_table.recordlookup('chain_id')
    prefix_header = ('trade', 'underlying', 'cost', 'active')
    init_header = ('target_win', 'cr', 'target_lose',
                   'netliq_win', 'netliq_flat', 'netliq_lose',
                   'pnl_win', 'pnl_flat', 'pnl_lose')
    accr_header = tuple('accr_' + x for x in init_header)
    header = prefix_header + init_header + accr_header
    chains_rows = [header]
    for chain_id, rows in chain_map.items():
        rows = list(rows)
        chain_table = AddOrderTotals(WrapRecords(rows))
        active, basis, init_cr, accr_cr = GetChainAmounts(chain_table)

        underlying = rows[0].underlying
        trade = "{}.{:%y%m%d}-{}".format(
            underlying,
            rows[0].datetime,
            "{:%y%m%d}".format(rows[-1].datetime) if not active else 'now')

        # Compute cost rows; fraction of the credit for taking off winners.
        init_costs, accr_costs = CalculateExitRow(basis, init_cr, accr_cr)
        row = (trade, underlying, basis, active) + init_costs + accr_costs
        chains_rows.append(row)

        if verbose:
            print("* {} ({})".format(trade, chain_id))
            print(chain_table.lookallstr())
            print(petl.wrap([header, row]).lookallstr())

    # Keep only the active position and folder the cost rows on top of each
    # other.
    fold_rows = [('trade', 'underlying', 'cost', 'crtype') + init_header]
    active_table = (petl.wrap(chains_rows)
                    .selecttrue('active')
                    .cutout('active')
                    .sort('trade'))
    for r in active_table.records():
        rt = list(r)
        init_costs = rt[3:12]
        accr_costs = rt[12:21]
        # Clear some of the repeated fields.
        init_costs[4] = ''
        init_costs[7] = ''
        fold_rows.append([r.trade, r.underlying, r.cost, 'accr'] + accr_costs)
        fold_rows.append(['', '', '', 'init'] + init_costs)
        fold_rows.append([])
    print(petl.wrap(fold_rows)
          .convert(('target_win', 'cr', 'target_lose',
                    'netliq_win', 'netliq_flat', 'netliq_lose',
                    'pnl_win', 'pnl_flat', 'pnl_lose'),
                   lambda v: v.quantize(Q) if isinstance(v, Decimal) else v)
          .lookallstr())


# TODO(blais): Add days since trade started.
# TODO(blais): Detect if a filter was applied on the account statemnet -> warn to remove
# TODO(blais): Render to HTML convenience
# TODO(blais): Add the missing expirations!
# TODO(blais): Render P/L over all trades.
# TODO(blais): Make it possible to input the P50 on entry, somehow.


if __name__ == '__main__':
    main()
