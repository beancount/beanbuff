"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

import collections
import os
from os import path
from typing import Callable, Dict, List, Optional, Tuple

from johnny.base.etl import petl, Table

from beanbuff.data import chains
from beanbuff.data import match
from beanbuff.tastyworks import tastyworks_transactions
from beanbuff.ameritrade import thinkorswim_transactions


# Args:
#   filename: str
# Returns:
#   account: str
#   sortkey: str
#   module: ModuleType
MatchFn = Callable[[str], Optional[Tuple[str, str, callable]]]


def FindFiles(fileordirs: List[str],
              matchers: List[MatchFn]) -> Dict[str, callable]:
    """Read in the transations log files from given directory and filenames."""

    # If input is empty, use the CWD.
    if not fileordirs:
        fileordirs = [os.getcwd()]
    elif isinstance(fileordirs, str):
        fileordirs = [fileordirs]

    # Find all the files for each account.
    byaccount = collections.defaultdict(list)

    def MatchStore(filename: str):
        for matcher in matchers:
            r = matcher(filename)
            if r:
                account, sortkey, parser = r
                byaccount[account].append((sortkey, filename, parser))

    for filename in fileordirs:
        if path.isdir(filename):
            for fn in os.listdir(filename):
                MatchStore(path.join(filename, fn))
        else:
            MatchStore(filename)

    # Select the latest matched file for each account.
    matchdict = {}
    for account, matchlist in byaccount.items():
        _, filename, parser = next(iter(sorted(matchlist, reverse=True)))
        matchdict[account] = (filename, parser)

    return matchdict


def GetTransactions(fileordirs: List[str]) -> Tuple[Table, List[str]]:
    """Find files and parse and concatenate contents."""

    matches = FindFiles(
        fileordirs, [
            tastyworks_transactions.MatchFile,
            thinkorswim_transactions.MatchFile
        ])

    filenames = []
    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        transactions, _ = parser(filename)
        if not transactions:
            continue
        filenames.append(filename)

        # Note: These need to be processed by file, separately.
        # TODO(blais): Process 'other' transactions.
        transactions = match.Match(transactions)
        transactions = chains.Group(transactions)
        tables.append(transactions)

    table = petl.cat(*tables) if tables else petl.empty()
    return table, filenames
