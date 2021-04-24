"""Module to pair up opening and closing transactions.
"""

__author__ = 'Martin Blais <blais@furius.ca>'

import collections
import hashlib
from decimal import Decimal
from typing import Dict, Tuple

import petl
from petl import Table
petl.config.look_style = 'minimal'
petl.config.failonerror = True


ZERO = Decimal(0)


def Match(transactions: Table) -> Dict[str, str]:
    """Compute a mapping of transaction ids to matches.

    This code will run through a normalized transaction log (see
    `transactions.md`) and match trades that reduce other ones. It will produce
    a mapping of (transaction-id, match-id). Each `match-id` will be a stable
    unique identifier across runs.
    """
    # Create a mapping of transaction ids to matches.
    invs = collections.defaultdict(NanoInventory)
    match_map = {}
    for rec in transactions.records():
        instrument_key = (rec.underlying, rec.expiration, rec.expcode, rec.side, rec.strike)
        inv = invs[instrument_key]
        matched, match_id = inv.match(rec.quantity, rec.transaction_id)
        match_map[rec.transaction_id] = match_id

    # Apply the mapping to the table.
    matched_transactions = (
        transactions
        .addfield('match_id', lambda r: match_map[r.transaction_id]))

    if 1:
        def g(grouper):
            for row in grouper:
                print(row)
            print()
        _ = (matched_transactions
             .aggregate('match_id', g))
        print(_.lookallstr())

    return matched_transactions



class NanoInventory:
    """Simple inventory object which implements matching of lots of a single instrument.

    The method we implement here avoids having to split rows for partial
    matches. It simplifies the process of partial matches by joining together
    partial reductions, e.g., the following sequences of changes are linked
    together:

      [+1, +1, -1, -1]
      [+1, +1, -2]
      [+2, -1, -1]
      [+1, -2, +1]
      [+2, -1, -2, +1]

    Basically as long as there is a reduction against an existing position, the
    same match id is used. The match id is derived from the opening position. An
    improvement in resolution would split some of these matches, e.g.,

      [+1, +1, -1, -1] --> [+1, -1], [-1, -1]
      [+2, -1, -2, +1] --> [+2, -1, -1], [-1, +1]

    but if you're fine with a bit more linkage, this will do.
    """

    def __init__(self):
        # The current quantity of the instrument.
        self.quantity = ZERO

        # The current match id being assigned.
        self.match_id: str = None

    def match(self, quantity: Decimal, transaction_id: str) -> Tuple[Decimal, str]:
        """Match the given change against the inventory state.
        Return the signed matched size and match id to apply.
        """
        # Add to the existing quantity; keep the same transaction id.
        if self.match_id is None:
            self.match_id = _create_match_id(transaction_id)

        if self.quantity * quantity >= ZERO:
            matched = ZERO
        elif abs(quantity) < abs(self.quantity):
            matched = quantity
        else:
            matched = -self.quantity

        self.quantity += quantity
        match_id = self.match_id

        if self.quantity == ZERO:
            self.match_id = None

        return (matched, match_id)

    def expire(self, transaction_id: str) -> Tuple[Decimal, str]:
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        matched = -self.quantity
        self.quantity = ZERO

        match_id = (_create_match_id(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None
        return (matched, match_id)


def _create_match_id(transaction_id: str) -> str:
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "&{}".format(md5.hexdigest())
