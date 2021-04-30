"""Parse positions files.

This produces a standardized positions and a separate
non-transaction log. See bottom for public entry points.
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


from beanbuff.data import discovery
from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.tastyworks import tastyworks_positions
from beanbuff.ameritrade import thinkorswim_positions


def GetPositions(fileordirs: List[str]) -> Table:
    """Find files and parse and concatenate contents."""

    matches = discovery.FindFiles(
        fileordirs,
        [tastyworks_positions.MatchFile,
         #thinkorswim_positions.MatchFile
         ])

    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        positions = parser(filename)
        tables.append(positions)

    return petl.cat(*tables)


@click.command()
@click.argument('fileordirs', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--verbose', '-v', is_flag=True)
def main(fileordirs: List[str], verbose: bool):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    positions = GetPositions(fileordirs)
    if positions:
        logging.fatal("No input files to read from the arguments.")
        return

    print(positions.lookallstr())


if __name__ == '__main__':
    main()
