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


from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.data.etl import petl, Table, Record, WrapRecords

from beanbuff.tastyworks import tastyworks_positions
from beanbuff.ameritrade import thinkorswim_transactions


# Available modules to import transactions from.
_MODULES = [
    tastyworks_positions,
    #thinkorswim_transactions,
]


# TODO(blais): Factor this out and reuse with the other one.
def FindAndReadFiles(filenames: List[str], debug: bool=False) -> Optional[Table]:
    """Read in the data files from directory names and filenames."""

    if not filenames:
        filenames = [os.getcwd()]

    # Find all the files.
    tables = []
    for filename in filenames:
        found_list = []
        if path.isdir(filename):
            for module in _MODULES:
                latest = module.FindLatestPositionsFile(filename)
                if latest:
                    found_list.append((latest, module))
        else:
            for module in _MODULES:
                if module.IsPositionsFile(filename):
                    found_list.append((filename, module))
                    break
        for found, module in found_list:
            logging.info("Process '%s' with module '%s'", found, module.__name__)

            pos_table = module.GetPositions(found)
            tables.append(pos_table)

    if not tables:
        return None

    table = petl.cat(*tables)
    if debug:
        print(table.lookallstr())
    return table


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path(resolve_path=True, exists=True))
@click.option('--verbose', '-v', is_flag=True)
def main(filenames: List[str], verbose: bool):
    """Main program."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    # Read the input files.
    pos_table = FindAndReadFiles(filenames, debug=0)
    if not pos_table:
        logging.fatal("No input files to read from the arguments.")
        return

    print(pos_table.lookallstr())


if __name__ == '__main__':
    main()
