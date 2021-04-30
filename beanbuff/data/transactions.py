"""Parse transactions history files.

This produces a standardized transactions history log and a separate
non-transaction log. See bottom for public entry points.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Callable, List, Optional, Tuple, Iterator, Optional, Iterable
import types
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser
from more_itertools import first

from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data import discovery
from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.tastyworks import tastyworks_transactions
from beanbuff.ameritrade import thinkorswim_transactions


ZERO = Decimal(0)
Q1 = Decimal('1')
Q = Decimal('0.01')


# TODO(blais): Move all this to consolidated.


def ChainName(rec: Record) -> str:
    """Generate a unique chain name."""
    return "{}.{}.{}.{}".format(
        rec.account,
        "{:%y%m%d}".format(rec.mindate),
        "{:%y%m%d}".format(rec.maxdate) if not rec.active else 'now',
        rec.underlying)


def InitialCredits(pairs: Iterator[Tuple[str, Decimal]]) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    sum_order_id = None
    sum_first_order = ZERO
    for order_id, cost in pairs:
        if sum_order_id is None or order_id < sum_order_id:
            sum_order_id = order_id
            sum_first_order = cost
        elif order_id == sum_order_id:
            sum_first_order += cost
    return sum_first_order


def OptSum(numbers: Iterable[Optional[Decimal]]) -> Decimal:
    return sum(number for number in numbers if number is not None)


def TransactionsToChains(transactions: Table) -> Table:
    """Convert transactions table into a table of chains."""

    # Aggregate across chains, keeping the row type.
    agg = {
        'account': ('account', first),
        'mindate': ('datetime', lambda g: min(g).date()),
        'maxdate': ('datetime', lambda g: max(g).date()),
        'underlying': ('underlying', first),
        'cost': ('cost', sum),
        'init': (('order_id', 'cost'), InitialCredits),
        'commissions': ('commissions', sum),
        'fees': ('fees', sum),

        #'p_cost': ('p_cost', OptSum),
        'net_liq': ('p_net_liq', OptSum),
        #'pnl': ('p_pnl', OptSum),
        'pnl_day': ('p_pnl_day', OptSum),
    }
    typed_chains = (
        transactions
        .replace('commissions', None, ZERO)
        .replace('fees', None, ZERO)
        .aggregate(['chain_id', 'rowtype'], agg)
        .sort('underlying'))


    # Split historical and active chains aggregate and join them to each other.
    histo, mark = typed_chains.biselect(lambda r: r.rowtype == 'Trade')
    chains = petl.outerjoin(
        (histo
         .cutout('rowtype', 'net_liq', 'pnl_day')
         .rename('cost', 'accr')),
        (mark
         .cut('chain_id', 'cost', 'net_liq', 'pnl_day')
         .addfield('active', True)), key='chain_id')

    # Finalize the table, filling in missing values and adding per-chain fields.
    chains = (
        chains
        .replace('cost', None, ZERO)
        .convert('cost', lambda v: -v)
        .replace('active', None, False)
        .addfield('days', lambda r: (r.maxdate - r.mindate).days)
        .addfield('chain_name', ChainName)
        .sort(['underlying', 'maxdate']))

    return chains


def FormatActiveChains(chains: Table) -> Table:
    """Format and render the trades table iin a readable way."""

    # Clean up and format the table a bit.
    chains = (
        chains
        .cut('account', 'chain_name', 'chain_id',
             'underlying', 'mindate', 'maxdate',
             'init', 'accr', 'cost', 'commissions', 'fees',
             'days',
             'net_liq', 'pnl_day')
        .rename('commissions', 'commis'))

    # Add P50 column.
    #
    # TODO(blais): Join this with offline system where I can enter the actual
    # initial conditions.
    chains = (
        chains
        .addfield('p50', Decimal('0.80')))

    # Add target columns.
    chains = (
        chains
        .addfield('tgtwin', lambda r: ShortNum(r.init * win_frac))
        .addfield('tgtloss', lambda r: ShortNum(r.init * win_frac * LoseFrac(r.p50)))
        .addfield('accr_tgtwin', lambda r: ShortNum(r.accr * win_frac))
        .addfield('accr_tgtloss', lambda r: ShortNum(r.accr * win_frac * LoseFrac(r.p50))))

    chains = chains.addfield('---', '')

    # Add Net Liq columns.
    chains = (
        chains
        .addfield('nla/win', lambda r: ShortNum(-r.accr + r.accr_tgtwin))
        .addfield('nl/win', lambda r: ShortNum(-r.accr + r.tgtwin))
        .addfield('nl/flat', lambda r: ShortNum(-r.accr))
        .addfield('nl/loss', lambda r: ShortNum(-r.accr + r.tgtloss))
        .addfield('nla/loss', lambda r: ShortNum(-r.accr + r.accr_tgtloss)))

    chains = chains.addfield('---', '')

    chains = (
        chains
        .addfield('net_liq', lambda r: r.net_liq)
        .cutout('net_liq')

        .addfield('chain_pnl', lambda r: r.net_liq - r['nl/flat'])
        .addfield('tgtinit%', PercentTargetInitial)
        .addfield('tgtaccr%', PercentTargetAccrued)

        .addfield('---', '')
        .addfield('pnl_day', lambda r: r.pnl_day)
        .cutout('pnl_day')
        )

    # Remove accrued targets, it's too much.
    chains = (chains
              .cutout('accr_tgtwin', 'accr_tgtloss'))

    return chains


def PercentTargetInitial(r: Record) -> Decimal:
    """Compute the % of target reached."""
    if r.chain_pnl > ZERO:
        value = r.chain_pnl / r.tgtwin if r.tgtwin else ZERO
    else:
        value = r.chain_pnl / abs(r.tgtloss) if r.tgtloss else ZERO
    return value.quantize(Q)


def PercentTargetAccrued(r: Record) -> Decimal:
    """Compute the % of target reached."""
    if r.chain_pnl > ZERO:
        value = r.chain_pnl / r.accr_tgtwin if r.accr_tgtwin else ZERO
    else:
        value = r.chain_pnl / abs(r.accr_tgtloss) if r.accr_tgtloss else ZERO
    return value.quantize(Q)


def ShortNum(number: Decimal) -> str:
    """Make the target numbers compact, they do have to be precise."""
    return number.quantize(Q1)


def LoseFrac(p: Decimal) -> Decimal:
    """Compute exit fraction based on P50%."""
    return -p / (1 - p)


win_frac = Decimal('0.50')
p50 = Decimal('0.80')


def GetTransactions(fileordirs: List[str]) -> Table:
    """Find files and parse and concatenate contents."""

    matches = discovery.FindFiles(
        fileordirs,
        [tastyworks_transactions.MatchFile,
         thinkorswim_transactions.MatchFile])

    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        transactions, _ = parser(filename)

        # Note: These need to be processed by file, separately.
        # TODO(blais): Process 'other' transactions.
        transactions = match.Match(transactions)
        transactions = chains.Group(transactions)
        tables.append(transactions)

    return petl.cat(*tables)


def GetActiveChains(transactions: Table) -> Table:
    """Filter the transactions to active chains only."""
    chains = TransactionsToChains(transactions)
    active_chains = (chains
                     .selecteq('active', True))
    return (FormatActiveChains(active_chains)
            .sort(['account', 'chain_name']))


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--html', type=click.Path(exists=False))
@click.option('--verbose', '-v', is_flag=True)
@click.option('--no-equity', is_flag=True)
def main(fileordirs: List[str], html: str, verbose: bool, no_equity: bool=True):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    transactions = GetTransactions(fileordirs)
    if not transactions:
        logging.fatal("No input files to read from the arguments.")
        return

    # Remove equity if desired.
    #
    # TODO(blais): Handle this by subtracting existing transactions from the
    # Ledger instead.
    if no_equity:
        transactions = (transactions
                        .select(lambda r: r.instype != 'Equity'))

    if 0:
        print(transactions.lookallstr()); raise SystemExit

    # Aggregate the transactions table into chains.
    chains = GetActiveChains(transactions)
    if html:
        chains.tohtml(html)
    print(chains.lookallstr())


# TODO(blais): Add EXPIRATIONS now!! I get incorrect output for TOS.

# TODO(blais): Remove orders from previous file.
# TODO(blais): Put generation time in file.

# TODO(blais): Split up chains between expirations?

# TODO(blais): Add days since trade started.
# TODO(blais): Render to HTML convenience
# TODO(blais): Add the missing expirations!
# TODO(blais): Render P/L over all trades.
# TODO(blais): Make it possible to input the P50 on entry, somehow.
# TODO(blais): Fix futures positions.

# TODO(blais): Join with the positions table.
# TODO(blais): Calculate metrics (P/L per day).

# TODO(blais): Add average days in trade; scatter P/L vs. days in analysis.

# TODO(blais): Complete this, for the details page of a vertical.
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


if __name__ == '__main__':
    main()
