#!/usr/bin/env python3
"""Compute total portfolio greeks and notional exposure.

This script parses the "Position Statementa" CSV file download you can export
from ThinkOrSwim. This offers a manually triggered "poor man's" risk monitor,
which you can use for spot checks over your entire portfolio. (Unfortunately, TD
does not provide you with risk management tools so this is the basic thing you
can do to get a sense of aggregate portfolio risk.)

Assumptions:

- You have groups of assets, and have "Show Groups" enabled.
- You have inserted the columns listed under 'FIELDS' below, including the
  various greeks and notional values to sum over.

Instructions:

- Go to the "Monitor >> Activity and Positions" tab.
- Turn on "Beta Weighting" on your market proxy of choice (e.g. SPY).
- Expand all the sections with the double-down arrows (somehow the export only
  outputs the expanded groups).
- Click on "Position Statement" hamburger menu and "Export to File...".
- Run the script with the given file.

The output will include:

- A consolidated position detail table.
- A table with the extrema for each greek or columns.
"""

import argparse
from decimal import Decimal
import logging
import re
from typing import List, Tuple, Optional

import click
import petl
petl.config.look_style = 'minimal'


Table = petl.Table
Record = petl.Record
Group = Tuple[str, str, Table]
Q = Decimal("0.01")


# The set of fields to produce aggregations over.
FIELDS = ['Delta', 'Gamma', 'Theta', 'Vega', 'Beta', 'Net Liq', 'P/L Open', 'P/L Day']


def SplitGroups(table: Table) -> List[Group]:
    group_list: List[Group] = []
    name, subname, group = None, None, []
    for row in table:
        # Skip all empty rows.
        if not row:
            continue

        # Match group header.
        if len(row) == 1:
            # Reset the current group.
            match = re.fullmatch(r'Group "(.*)"', row[0])
            if match:
                if name and subname and group:
                    group_list.append((name, subname, petl.wrap(group)))
                name, subname, group = match.group(1), None, []
                continue

            # Skip useless header.
            match = re.match(r"(Equities) and Equity Options", row[0])
            if match:
                if name and subname and group:
                    group_list.append((name, subname, petl.wrap(group)))
                subname, group = match.group(1), []
                continue
            match = re.match(r"(Futures) and Futures Options", row[0])
            if match:
                if name and subname and group:
                    group_list.append((name, subname, petl.wrap(group)))
                subname, group = match.group(1), []
                continue

        group.append(row)
    if name and subname and group:
        group_list.append((name, subname, petl.wrap(group)))
    return group_list


def ParseNumber(string: str) -> Decimal:
    """Parse a single number string."""
    if string == 'N/A (Split Position)':
        return Decimal('0')
    sign = 1
    match = re.match(r"\((.*)\)", string)
    if match:
        sign = -1
        string = match.group(1)
    return Decimal(string.replace('$', '').replace(',', '')).quantize(Decimal("0.01")) * sign


def ConsolidatePositionStatement(
        table,
        reference: Optional[Decimal] = None,
        debug_tables: bool = False) -> Tuple[Table, Table]:
    """Consolidate all the subtables in the position statement."""

    # Aggregator. Note: You need to have these columns shown in the UI, for all
    # groups.
    sums = {name: (name, sum) for name in FIELDS}

    # Prepare tables for aggregation, inserting groups and stripping subtables
    # (no reason to treat Equities and Futures distinctly).
    groups = SplitGroups(table)
    tables = []
    for name, subname, gtable in groups:
        ftable = (gtable
                  # Remove redundant detail.
                  .select(lambda r: bool(r['BP Effect']))
                  # Convert numbers to numbers.
                  .convert(FIELDS, ParseNumber)
                  # Select just the additive numerical fields.
                  .cut(['Instrument'] + FIELDS)
                  # Add group to the resulting table.
                  .addfield('Group', name, index=0)
                  .addfield('Type', subname, index=1)
                  )
        tables.append(ftable)

        if debug_tables:
            print(ftable.lookallstr())
            print(ftable.aggregate(key=None, aggregation=sums))
            print()

    # Consolidate the table.
    atable = petl.cat(*tables)

    # Add delta-equivalent notional value.
    if reference:
        atable = (atable
                  .addfield('Notional', lambda r: (r.Delta * reference).quantize(Q)))
        sums['Notional'] = ('Notional', sum)

    # Aggregate the entire table to a single row.
    totals = (atable
              .aggregate(key=None, aggregation=sums))

    return atable, totals


def Report(atable: Table, totals: Table,
           top_k: int = 5):
    """Print all the desired aggregations and filters."""

    print("# Position Statement\n")

    fields = list(FIELDS)
    if 'Notional' in atable.header():
        fields.append('Notional')

    # Concatenate totals to consolidated table.
    empty_row = petl.wrap([['Group', 'Type', 'Instrument'] + fields,
                           ['---'] * (len(fields) + 3)])
    consolidated = petl.cat(atable.convert(fields, float),
                            empty_row,
                            (totals
                             .convert(fields, float)
                             .addfield('Group', 'Totals', index=0)
                             .addfield('Type', '*', index=1)
                             .addfield('Instrument', '*', index=2)))

    # Print table detail.
    print("## Consolidated Position Detail\n")
    print(consolidated.lookallstr())

    # Print top-K largest positive and negative greeks risk.
    top_tables = []
    sep = '-/-'
    print("## Largest Values\n")
    for field in fields:
        stable = (atable
                  .sort(field, reverse=True)
                  .convert(field, lambda v: float(v))
                  .cut('Instrument', field)
                  .rename('Instrument', ''))
        head_table = stable.head(top_k)
        empty_table = petl.wrap([stable.header(), ['', '...', '...']])
        tail_table = stable.tail(top_k)
        sstable = (petl.cat(head_table, empty_table, tail_table)
                   .addfield(sep, ''))
        top_tables.append(sstable)
        #print(sstable.lookallstr())
    top_table = petl.annex(*top_tables)
    print(top_table.lookallstr())


@click.command()
@click.argument('positions_csv')
@click.option('--reference', '-r', type=Decimal, default=None,
              help="Price of the beta-weighted reference applied to the downloaded file.")
def main(positions_csv, reference):
    # If the reference isn't given, attempt to get tit from
    if reference is None:
        try:
            from beanprice.sources import yahoo
        except ImportError:
            pass
        else:
            source = yahoo.Source()
            sprice = source.get_latest_price("SPY")
            reference = sprice.price

    # Read positions statement and consolidate it.
    table = petl.fromcsv(positions_csv)
    atable, totals = ConsolidatePositionStatement(table, reference)
    Report(atable, totals, 10)


if __name__ == '__main__':
    main()
