"""Consolidated adjusted positions table.

This joins a normalized transactions log with a normalized positions table to
provide a chain-based view of P/L adjusted to realized histories on the trade
chains.
"""

import collections
from decimal import Decimal
from os import path
from typing import Any, Callable, List, Optional, Tuple, Iterator, Iterable, Set
import types
import datetime
import hashlib
import logging
import pprint
import re
import os
import sys

from more_itertools import first
import click
from dateutil import parser

from beancount import loader
from beancount.core import data

from beanbuff.data.etl import petl, Table, Record, WrapRecords
from beanbuff.data import transactions as transactions_mod
from beanbuff.data import positions as positions_mod
from beanbuff.data import beansym


# Decimal constants.
ZERO = Decimal(0)
Q1 = Decimal('1')
Q = Decimal('0.01')


# Fraction of credits received we aim to collect.
WIN_FRAC = Decimal('0.50')

# Probability of hitting 50%.
# TODO(blais): We setup a default probability.
P50 = Decimal('0.80')


def ChainName(rec: Record) -> str:
    """Generate a unique chain name."""
    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    return ".".join([
        rec.account,
        "{:%y%m%d_%H%M%S}".format(rec.mindate),
        rec.underlying])


def InitialCredits(pairs: Iterator[Tuple[str, Decimal]]) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    first_order_id = None
    first_order_sum = ZERO
    for order_id, cost in pairs:
        if first_order_id is None or order_id is None or order_id < first_order_id:
            first_order_id = order_id
            first_order_sum = cost
        elif order_id == first_order_id:
            first_order_sum += cost
    return first_order_sum


def OptSum(numbers: Iterable[Optional[Decimal]]) -> Decimal:
    return sum((number if isinstance(number, Decimal) else  ZERO) for number in numbers)


def TransactionsToChains(transactions: Table) -> Table:
    """Convert transactions table into a table of chains."""

    # Aggregate across chains, keeping the row type.
    agg = {
        'account': ('account', first),
        'mindate': ('datetime', lambda g: min(g)),
        'maxdate': ('datetime', lambda g: max(g)),
        'underlying': ('underlying', first),
        'cost': ('cost', sum),
        'init': (('order_id', 'cost'), InitialCredits),
        'commissions': ('commissions', sum),
        'fees': ('fees', sum),

        'net_liq': ('net_liq', OptSum),
        'pnl_day': ('pnl_day', OptSum),
        # 'pnl': ('pnl_open', OptSum),
        # 'cost': ('pos_cost', OptSum), # Note: Could validate they match.
    }
    typed_chains = (
        transactions
        .replace('commissions', None, ZERO)
        .replace('fees', None, ZERO)

        # Aggregate by chain and row type.
        .addfield('ismark', lambda r: r.rowtype == 'Mark')
        .aggregate(['chain_id', 'ismark'], agg)

        .sort('underlying'))

    # Split historical and active chains aggregate and join them to each other.
    mark, histo = typed_chains.biselect(lambda r: r.ismark)
    chains = petl.outerjoin(
        (histo
         .cutout('ismark', 'net_liq', 'pnl_day')
         .rename('cost', 'accr')),
        (mark
         .cut('chain_id', 'cost', 'net_liq', 'pnl_day')
         .addfield('status', 'ACTIVE')), key='chain_id')

    # Finalize the table, filling in missing values and adding per-chain fields.
    chains = (
        chains
        .replace('cost', None, ZERO)
        .convert('cost', lambda v: -v)
        .replace('status', None, 'DONE')
        .addfield('days', lambda r: (r.maxdate - r.mindate).days)
        .addfield('chain_name', ChainName)
        .convert('mindate', lambda v: v.date())
        .convert('maxdate', lambda v: v.date())
        .sort(['underlying', 'maxdate']))

    return chains


def FormatActiveChains(chains: Table) -> Table:
    """Format and render the trades table iin a readable way."""

    # Clean up and format the table a bit.
    chains = (
        chains
        .cut('chain_name', 'chain_id', 'account', 'status',
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
        .addfield('p50', P50))

    # Add target columns.
    chains = (
        chains
        .addfield('tgtwin', lambda r: ShortNum(r.init * WIN_FRAC))
        .addfield('tgtloss', lambda r: ShortNum(r.init * WIN_FRAC * LoseFrac(r.p50)))
        .addfield('accr_tgtwin', lambda r: ShortNum(r.accr * WIN_FRAC))
        .addfield('accr_tgtloss', lambda r: ShortNum(r.accr * WIN_FRAC * LoseFrac(r.p50))))

    # Add Net Liq columns.
    chains = (
        chains
        .addfield('nla/win', lambda r: ShortNum(-r.accr + r.accr_tgtwin))
        .addfield('nl/win', lambda r: ShortNum(-r.accr + r.tgtwin))
        .addfield('nl/flat', lambda r: ShortNum(-r.accr))
        .addfield('nl/loss', lambda r: ShortNum(-r.accr + r.tgtloss))
        .addfield('nla/loss', lambda r: ShortNum(-r.accr + r.accr_tgtloss)))

    chains = (
        chains

        .addfield('net_liq', lambda r: r.net_liq)
        .cutout('net_liq')

        .addfield('chain_pnl', lambda r: (r.net_liq or ZERO) - r['nl/flat'])
        .addfield('tgtinit%', PercentTargetInitial)
        .addfield('tgtaccr%', PercentTargetAccrued)

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


# TODO(blais): Move this to a common place.
def SynthesizeSymbol(r: Record) -> str:
    """Remove the symbol columns and replace them by a single symbol."""
    return str(beansym.FromColumns(r.underlying,
                                   r.expiration,
                                   r.expcode,
                                   r.putcall,
                                   r.strike,
                                   r.multiplier))


def DebugPrint(tabledict):
    for name, table in tabledict.items():
        filename = "/tmp/{}.csv".format(name)
        table.sort().tocsv(filename)


_TEMPLATE = """
<html>
<head>

<script type="text/javascript"
        src="https://code.jquery.com/jquery-3.5.1.js"></script>
<script type="text/javascript"
        src="https://cdn.datatables.net/1.10.24/js/jquery.dataTables.min.js"></script>

<link rel="stylesheet"
      href="https://cdn.datatables.net/1.10.24/css/jquery.dataTables.min.css">
<link rel="preconnect" href="https://fonts.gstatic.com">
<link href="https://fonts.googleapis.com/css2?family=Roboto+Condensed&display=swap"
      rel="stylesheet">

<script>
  $(document).ready(function() {
      $('#positions').DataTable({"pageLength": 200});
  });
</script>

<style>
body {
  font-family: 'Roboto Condensed', sans-serif;
  font-size: 9px;
}
</style>

</head>
<body>

TABLE

</body>
</html>
"""

def ToHtmlString(table: Table):
    table = (table
             .cutout('---')
             .cutout('---')
             .cutout('---'))
    sink = petl.MemorySource()
    table.tohtml(sink)
    html = sink.getvalue().decode('utf8')
    html = re.sub("class='petl'", "class='display compact cell-border' id='positions'", html)
    return html


def ToHtml(table: Table, filename: str):
    html = ToHtmlString(table)
    _TEMPLATE.replace('TABLE', html)
    with open(filename, 'w') as ofile:
        print(html, file=ofile)


def GetOrderIdsFromLedger(filename: str) -> Set[str]:
    """Read a list of order ids to remove from a Beancount ledger."""
    filename = path.abspath(filename)
    order_ids = set()
    entries, _, __ = loader.load_file(filename)
    match_link = re.compile('order-(.*)').match
    for entry in data.filter_txns(entries):
        # Ignore files other than the root file.
        # This is because I place trading in included files.
        if entry.meta['filename'] != filename:
            continue
        for link in entry.links:
            match = match_link(link)
            if match:
                order_ids.add(match.groups(1))
    return order_ids


def ConsolidateChains(fileordirs: str, ledger: Optional[str]):
    """Read all the data and join it and consolidate it."""

    # Read the transactions files.
    transactions, filenames = transactions_mod.GetTransactions(fileordirs)
    for fn in filenames:
        logging.info("Read file '%s'", fn)
    if not transactions:
        logging.fatal("No input files to read from the arguments.")

    # Remove transactions from the Ledger if there are any.
    if ledger:
        # Remove rows with those order ids.
        order_ids = GetOrderIdsFromLedger(ledger)
        transactions = (transactions
                        .selectnotin('order_id', order_ids))

    # Read the positions files.
    positions, filenames = positions_mod.GetPositions(fileordirs)
    for fn in filenames:
        logging.info("Read file '%s'", fn)

    # TODO(blais): Do away with this eventually.
    transactions = (transactions
                    .select(lambda r: r.instype != 'Equity'))

    # Keep only the open options positions in the transactions log.
    transactions = (transactions
                    .addfield('symbol', SynthesizeSymbol))

    # If we have a valid positions file, we join it in.
    # This script should work or without one.
    if positions.nrows() > 0:
        positions = (positions
                     # Add column to match only mark rows to position rows.
                     .addfield('rowtype', 'Mark')

                     # Remove particular groups.
                     # TODO(blais): Make this configurable.
                     .select('group', lambda v: v is None or not re.match(r'Core\b.*', v)))

        # Join positions to transactions.
        transactions = (
            petl.outerjoin(transactions, positions,
                           key=['account', 'rowtype', 'symbol'], rprefix='p_')

            # Rename some of the added columns.
            .rename('p_net_liq', 'net_liq')
            .rename('p_cost', 'pos_cost')
            .rename('p_pnl_open', 'pnl_open')
            .rename('p_pnl_day', 'pnl_day')

            .cutout('p_group', 'p_quantity', 'p_price', 'p_mark', 'pos_cost'))
    else:
        # Add columns that would be necessary from the positions table.
        transactions = (transactions
                        .addfield('net_liq', None)
                        .addfield('pnl_day', None))

    # Convert to chains.
    chains = TransactionsToChains(transactions)

    # Clean up the chains and add targets.
    chains = FormatActiveChains(chains)

    return transactions, positions, chains


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--ledger', '-l', type=click.Path(exists=False),
              help="Remove transactions from Ledger. Requires order ids.")
@click.option('--html', type=click.Path(exists=False),
              help="Generate HTML output file.")
@click.option('--inactive', is_flag=True,
              help="Include inactive chains.")
def main(fileordirs: str, ledger: Optional[str], html: Optional[str], inactive: bool):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read in and consolidate all the data.
    _, __, chains = ConsolidateChains(fileordirs, ledger)

    # Remove inactive chains if requested.
    if not inactive:
        chains = (chains
                  .selecteq('status', 'ACTIVE'))

    # Output the table.
    print(chains.lookallstr())
    if html:
        ToHtml(chains, html)

    # Render some totals.
    agg = {
        'commissions': ('commis', sum),
        'fees': ('fees', sum),
    }
    if 'chain_pnl' in chains.fieldnames():
        agg['total_pnl'] = ('chain_pnl', sum)
    print(chains
          .aggregate(None, agg).lookallstr())


if __name__ == '__main__':
    main()
