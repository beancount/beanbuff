"""Module to pair up opening and closing transactions.
"""

__author__ = 'Martin Blais <blais@furius.ca>'

import datetime
from pprint import pprint
import collections
import itertools
import hashlib
from decimal import Decimal
from typing import Dict, Tuple, NamedTuple, Optional

from beanbuff.data.etl import petl, Table, Record


ZERO = Decimal(0)


class InstKey(NamedTuple):
    account: str
    instype: str
    underlying: str
    expiration: datetime.date
    expcode: Optional[str]
    putcall: str
    strike: Decimal
    multiplier: Decimal


def Match(transactions: Table) -> Dict[str, str]:
    """Compute a mapping of transaction ids to matches.

    This code will run through a normalized transaction log (see
    `transactions.md`) and match trades that reduce other ones. It will produce
    a mapping of (transaction-id, match-id). Each `match-id` will be a stable
    unique identifier across runs.
    """
    # Create a mapping of transaction ids to matches.
    invs = collections.defaultdict(FifoInventory)
    match_map = {}
    for rec in transactions.records():
        instrument_key = InstKey(
            rec.account,
            rec.instype, rec.underlying, rec.expiration, rec.expcode, rec.putcall,
            rec.strike, rec.multiplier)
        inv = invs[instrument_key]
        sign = (1 if rec.instruction == 'BUY' else -1)
        basis = rec.multiplier * rec.price
        _, __, match_id = inv.match(sign * rec.quantity, basis, rec.transaction_id)
        if match_id:
            match_map[rec.transaction_id] = match_id

    # Insert Mark rows to close out the positions virtually.
    closing_transactions = [
        ('account', 'transaction_id', 'rowtype', 'datetime', 'order_id',
         'instype', 'underlying', 'expiration', 'expcode', 'putcall', 'strike', 'multiplier',
         'effect', 'instruction', 'quantity', 'cost')
    ]
    idgen = iter(itertools.count(start=1))
    dt_mark = datetime.datetime.now().replace(microsecond=0)
    for key, inv in invs.items():
        quantity, basis, match_id = inv.position()
        if quantity != ZERO:
            (account, instype, underlying, expiration, expcode, putcall,
             strike, multiplier) = instrument_key
            mark_id = next(idgen)
            transaction_id = "^mark{:06d}".format(mark_id)
            order_id = "o{:06d}".format(mark_id)
            instruction = 'BUY' if quantity < ZERO else 'SELL'
            sign = -1 if quantity < ZERO else 1
            closing_transactions.append(
                (key.account, transaction_id, 'Mark', dt_mark, order_id,
                 key.instype, key.underlying, key.expiration, key.expcode, key.putcall,
                 key.strike, key.multiplier,
                 'CLOSING', instruction, abs(quantity), sign * basis))
            match_map[transaction_id] = match_id

    # Apply the mapping to the table.
    matched_transactions = (
        petl.cat(transactions, petl.wrap(closing_transactions))
        .addfield('match_id', lambda r: match_map[r.transaction_id]))

    return matched_transactions


def _CreateMatchId(transaction_id: str) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "&{}".format(md5.hexdigest())


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

    create_id_fn = staticmethod(_CreateMatchId)

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
            self.match_id = self.create_id_fn(transaction_id)

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
        if self.quantity == ZERO:
            return ZERO, None

        matched = -self.quantity
        self.quantity = ZERO

        match_id = (self.create_id_fn(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None
        return (matched, match_id)


class Lot(NamedTuple):
    """A single lot with basis."""
    quantity: Decimal
    basis: Decimal


class FifoInventory:
    """Simple inventory object which implements matching of lots of an instrument as FIFO.

    The method we implement here can split rows for partial matches, and
    maintains cost basis, so we can calculate the final cost of the remaining
    lots.
    """

    create_id_fn = staticmethod(_CreateMatchId)

    def __init__(self):
        # A list of insertion-ordered lots as (quantity, basis) pairs.
        self.lots: List[Lot] = []

        # The current match id being assigned.
        self.match_id: str = None

    def match(self, quantity: Decimal, cost: Decimal,
              transaction_id: str) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Match the given change against the inventory state.
        Return the absolute matched size, absolute basis, and match id to apply.
        """
        # Note: You could calculate unrealized P/L on matches.

        # Add to the existing quantity; keep the same transaction id.
        if self.match_id is None:
            self.match_id = self.create_id_fn(transaction_id)

        # Notes: `basis` and `matched` are positive.
        basis = ZERO
        matched = ZERO
        if not self.lots:
            # Adding to an empty inventory.
            self.lots.append(Lot(quantity, cost))
        else:
            # Calculate the sign of the current position.
            sign = 1 if self.lots[0].quantity >= 0 else -1
            if sign * quantity >= ZERO:
                # Augmentation on existing position.
                self.lots.append(Lot(quantity, cost))
            else:
                # Reduction.
                # Notes: lot_matched` and `remaining` are positive.
                remaining = -sign * quantity
                while self.lots and remaining > ZERO:
                    lot = self.lots.pop(0)

                    lot_matched = min(sign * lot.quantity, remaining)
                    matched += lot_matched
                    basis += lot_matched * lot.basis
                    remaining -= lot_matched

                    if lot_matched < sign * lot.quantity:
                        # Partial lot matched; reinsert remainder.
                        self.lots.insert(0, Lot(lot.quantity - sign * lot_matched, lot.basis))
                        break

                # Remaining quantity to insert to cross.
                if remaining != ZERO:
                    self.lots.append(Lot(-sign * remaining, cost))

        match_id = self.match_id
        if not self.lots:
            self.match_id = None

        return (matched, basis, match_id)

    def expire(self, transaction_id: str) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        if not self.lots:
            return ZERO, ZERO, None

        sign = 1 if self.lots[0].quantity >= 0 else -1
        matched = sign * sum(lot.quantity for lot in self.lots)
        basis = sign * sum(lot.quantity * lot.basis for lot in self.lots)
        self.lots = []

        match_id = (self.create_id_fn(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None

        return (matched, basis, match_id)

    def position(self) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Return the sum total quantity and cost basis."""
        if not self.lots:
            return ZERO, ZERO, None
        position = sum(lot.quantity for lot in self.lots)
        basis = sum(abs(lot.quantity) * lot.basis for lot in self.lots)
        return position, basis, self.match_id
