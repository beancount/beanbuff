"""Tastyworks - Parse transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Callable, List, Optional, Tuple
import types
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser


from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data import match
from beanbuff.data.etl import petl, Table, Record, WrapRecords

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
#     # Render a trade to something nicely readable.
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

# def GuessFileSource(filename: str) -> Tuple[str, types.ModuleType]:
#     """Guess the source of the given filename."""
#     with open(filename) as infile:
#         line = infile.readline()
#         if re.match(r'Date,Type,Action,Symbol', line):
#             return 'tastyworks', tastyworks_transactions
#         elif re.match('\ufeffAccount Statement for', line):
#             if re.search(r'Account Order History filtered by', infile.read()):
#                 raise ValueError("ThinkOrSwim account statement is filtered: '{}'. "
#                                  "Remove filter and start over.".format(filename))
#             else:
#                 return 'thinkorswim', thinkorswim_transactions
#         else:
#             raise ValueError(
#                 "Could not figure out the source of file: '{}'".format(filename))



# Available modules to import transactions from.
_MODULES = [
    tastyworks_transactions,
    thinkorswim_transactions,
]


def FindAndReadInputFiles(filenames: List[str], debug: bool=False) -> Optional[Table]:
    """Read in the data files from directory names and filenames."""

    if not filenames:
        filenames = [os.getcwd()]

    # Find all the files.
    tables = []
    for filename in filenames:
        found_list = []
        if path.isdir(filename):
            for module in _MODULES:
                latest = module.FindLatestTransactionsFile(filename)
                if latest:
                    found_list.append((latest, module))
        else:
            for module in _MODULES:
                if module.IsTransactionsFile(filename):
                    found_list.append((filename, module))
                    break
        for found, module in found_list:
            logging.info("Process '%s' with module '%s'", found, module.__name__)

            # TODO(blais): Process 'other' transactions.
            trades_table, _ = module.GetTransactions(found)
            trades_table = match.Match(trades_table)
            trades_table = chains.Group(trades_table)
            tables.append(trades_table)

    if not tables:
        return None

    table = petl.cat(*tables)
    if debug:
        print(table.lookallstr())
    return table


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('-v', '--verbose', is_flag=True)
@click.option('--no-equity', is_flag=True)
def main(filenames: List[str], verbose: bool, no_equity=True):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    trades_table = FindAndReadInputFiles(filenames, debug=0)
    if not trades_table:
        logging.fatal("No input files to read from the arguments.")
        return

    # Remove equity if desired.
    #
    # TODO(blais): Handle this by subtracting existing transactions from the
    # Ledger instead.
    if no_equity:
        trades_table = (trades_table
                        .select(lambda r: r.instype != 'Equity'))

    if 0:
        print(trades_table.lookallstr())

    # Group by chain and render.
    chain_map = trades_table.recordlookup('chain_id')
    prefix_header = ('account', 'underlying', 'trade', 'cost', 'active')
    init_header = ('target_win', 'cr', 'target_lose',
                   'netliq_win', 'netliq_flat', 'netliq_lose',
                   'pnl_win', 'pnl_flat', 'pnl_lose')
    accr_header = tuple('accr_' + x for x in init_header)
    header = prefix_header + init_header + accr_header
    chains_rows = [header]
    for chain_id, rows in chain_map.items():
        rows = list(rows)
        account = rows[0].account
        chain_table = AddOrderTotals(WrapRecords(rows))
        active, basis, init_cr, accr_cr = GetChainAmounts(chain_table)

        underlying = rows[0].underlying
        trade = "{}.{}.{:%y%m%d}-{}".format(
            underlying,
            account,
            rows[0].datetime,
            "{:%y%m%d}".format(rows[-1].datetime) if not active else 'now')

        # Compute cost rows; fraction of the credit for taking off winners.
        init_costs, accr_costs = CalculateExitRow(basis, init_cr, accr_cr)
        row = (account, underlying, trade, basis, active) + init_costs + accr_costs
        chains_rows.append(row)

        if verbose and active:
            print("* {} ({})".format(trade, chain_id))
            print(chain_table.lookallstr())
            print(petl.wrap([header, row]).lookallstr())

    # Keep only the active position and folder the cost rows on top of each
    # other.
    fold_header = ('account', 'trade', 'underlying', 'cost', 'crtype')
    fold_rows = [fold_header + init_header]
    active_table = (petl.wrap(chains_rows)
                    .selecttrue('active')
                    .cutout('active')
                    .sort(key=['account', 'underlying', 'trade']))
    for r in active_table.records():
        rt = list(r)
        init_costs = rt[4:13]
        accr_costs = rt[13:22]
        # Clear some of the repeated fields.
        init_costs[4] = ''
        init_costs[7] = ''
        row1 = [r.account, r.trade, r.underlying, r.cost, 'accr'] + accr_costs
        row2 = [''] * (len(fold_header)-1) + ['init'] + init_costs
        fold_rows.extend([row1, row2])
        fold_rows.append([''] * len(row1))

    final_table = (petl.wrap(fold_rows)
                   .convert(('target_win', 'cr', 'target_lose',
                             'netliq_win', 'netliq_flat', 'netliq_lose',
                             'pnl_win', 'pnl_flat', 'pnl_lose'),
                            lambda v: v.quantize(Q) if isinstance(v, Decimal) else v))

    _ = (final_table
         .replaceall(None, '')
         .tohtml("/home/blais/positions.html"))
    print(final_table.lookallstr())


# TODO(blais): Remove orders from previous file.
# TODO(blais): Put generation time in file.

# TODO(blais): Add days since trade started.
# TODO(blais): Render to HTML convenience
# TODO(blais): Add the missing expirations!
# TODO(blais): Render P/L over all trades.
# TODO(blais): Make it possible to input the P50 on entry, somehow.
# TODO(blais): Fix futures positions.

# TODO(blais): Join with the positions table.
# TODO(blais): Calculate metrics (P/L per day).

# TODO(blais): Add average days in trade; scatter P/L vs. days in analysis.


if __name__ == '__main__':
    main()
