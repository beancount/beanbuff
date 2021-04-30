"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

import collections
import os
from os import path
from typing import Callable, Dict, List, Optional, Tuple


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
            for filename in os.listdir(filename):
                MatchStore(filename)
        else:
            MatchStore(filename)

    # Select the latest matched file for each account.
    matchdict = {}
    for account, matchlist in byaccount.items():
        _, filename, parser = next(iter(sorted(matchlist, reverse=True)))
        matchdict[account] = (filename, parser)

    return matchdict
