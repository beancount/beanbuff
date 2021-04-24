"""Module to compute chains of related transactions over time.

Transactions are grouped together if

- They share the same order id (i.e., multiple legs of a single order
  placement).

- If they reduce each other, e.g., a closing transaction is grouped with its
  corresponding transaction.

- If they overlap in time (optionally). The idea is to identify "episodes" where
  the position went flat for the given instrument.

This code is designed to be independent of the source, as we went to be able to
do this on all options platforms, such as Ameritrade, InteractiveBrokers,
Vanguard and Tastyworks.
"""

__author__ = 'Martin Blais <blais@furius.ca>'


from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, NamedTuple, Union, Set, Optional, Iterator
import argparse
import functools
import collections
import datetime
import itertools
from os import path
import os
import logging
import re
import sys
from more_itertools import first, unzip
from pprint import pformat

import numpy
from dateutil import parser
import networkx as nx

from matplotlib import pyplot
import petl
petl.config.look_style = 'minimal'
#petl.compat.numeric_types = petl.compat.numeric_types + (Decimal,)

import ameritrade as td
from ameritrade import options
from ameritrade import utils


JSON = Union[str, int, float, Dict[str, 'JSON'], List['JSON']]
Side = str
Array = numpy.ndarray
Record = petl.Record


# Standard contract size.
CSIZE = 100


ZERO = Decimal()
Q = Decimal("0.01")


Trade = List[JSON]


class TxnType(Enum):
    TRADE = 'TRADE'
    EXPIRATION = 'EXPIRATION'
    MARK = 'MARK'

class Instruction(Enum):
    BUY = 'BUY'
    SELL = 'SELL'

class Effect(Enum):
    OPENING = 'OPENING'
    CLOSING = 'CLOSING'
    UNKNOWN = 'UNKNOWN'



Txn = NamedTuple("Txn", [

    # The date which the transaction occurred. Not the settlement date.
    ("date", datetime.datetime),

    # A unique transaction id by which we can identify this transaction.
    ("transactionId", str),

    # The order id used for the transaction.
    ("orderId", Optional[str]),

    # The match id linking closing transactions to opening ones.
    ("matchId", Optional[str]),

    # The type of transaction, trade or expirawtion.
    ("txnType", TxnType),

    # The symbol for the underlying instrument.
    ("symbol", str),

    # Whether to buy or sell the instrument.
    ("instruction", Optional[Instruction]),

    # Whether the transaction is opening or closing, if it is known. If it is
    # not known, we'll figure it out ourselves, but for that to work properly,
    # you will have to feed a complete list of opening-closing transactions.
    #
    # NOTE(blais): This is not strictly necessary but it can be used to trim
    # existing positions at the front of the time-series.
    ("effect", Effect),

    # The quantity bought or sold. If the quantity is sold, this number should
    # be negative.
    ("quantity", Decimal),

    # The price, in the quote currency, at which the instrument transacted.
    ("price", Decimal),

    # The amount in commissions.
    ("commissions", Decimal),

    # The amount in exchange and other fees.
    ("fees", Decimal),

    # The transactions also contain a 'isOpen' flag that allows you to separate
    # out the positions that are still open in the sequence (so you can compute
    # the full value of the entire position including marking the open position
    # to market).
    ("isOpen", bool),

    # Any object attached to this transaction. This can be the original
    # transaction object in the source data, or None.
    ("extra", Any)
])


IdGen = Iterator[str]


class TxnInventory:
    """Simple inventory object which implements matching."""

    def __init__(self):
        self.txns: List[Txn] = []

    def match(self, matching: Txn, idgen: IdGen) -> List[Txn]:
        """Match the given txn against the inventory state.
        Return a list of matches, including the given txn, if relevant."""

        matches = []
        while matching.quantity != 0 and self.txns:
            txn = self.txns[0]
            if txn.quantity * matching.quantity > 0:
                # Same sign; append the new transaction.
                break

            txn = self.txns.pop(0)
            if abs(txn.quantity) <= abs(matching.quantity):
                # Matched this existing position entirely.
                matchId = next(idgen)
                matches.append([
                    matching._replace(matchId=matchId,
                                      quantity=-txn.quantity),
                    txn._replace(matchId=matchId),
                ])
                matching = matching._replace(quantity=matching.quantity + txn.quantity)
                continue

            else:
                # Partially matches this existing position.
                matchId = next(idgen)
                matches.append([
                    matching._replace(matchId=matchId),
                    txn._replace(matchId=matchId,
                                 quantity=-matching.quantity),
                ])
                self.txns.insert(
                    0, txn._replace(quantity=txn.quantity + matching.quantity))
                matching = matching._replace(quantity=0)
                break

        if matching.quantity != 0:
            self.txns.append(matching)

        return matches

    def expire(self, matching: Txn, idgen: IdGen) -> List[Txn]:
        total_quantity = sum(txn.quantity for txn in self.txns)
        if abs(total_quantity) != abs(matching.quantity):
            logging.error("Expiring amount {} != position amount {}: {}".format(
                matching.quantity, total_quantity, matching))
            # We could not figure out the right sign; leave it as positive.
            sign = 1
        else:
            sign = -1 if total_quantity > 0 else 1
        matches = self.match(matching._replace(quantity=sign * matching.quantity), idgen)
        #assert not self.txns
        return matches

    def positions(self):
        return self.txns

    def __bool__(self):
        return bool(self.txns)


def GroupOrders(transactions: Iterable[Txn],
                by_matching=True,
                by_order=True,
                by_overlap=True) -> List[List[Txn]]:
    """Pair up closing and option and transactions connectedd by matching, order ids
    and/or overlapping time.

    Args:
      A list of transactions, adapted by whatever source of data you have.
    Returns:
      A sequence of transaction groups, each of which is a list of transactions.
    """

    # Find matching reductions, extracting trades. We do this before identifying
    # the strategies, because the various legs may be closed independently.
    matched = []

    def genids(prefix):
        for id_ in itertools.count():
            yield "{}{:02d}".format(prefix, id_)
    matchids = genids('match')

    # A mapping of option name to quantity and list of associated transactions
    # on that name.
    #
    # TODO(blais): You can get rid of the two level and simplify if you handle
    # spans within the graph, within the loop.
    unders = collections.defaultdict(lambda: collections.defaultdict(TxnInventory))

    # A mapping of underlying name to a mapping of option name to quantity and
    # transactions. We accumulate transactions both at the per-symbol level (for
    # quantity) and at the underlying level (for time span).
    spanlist = []

    counter = iter(itertools.count())
    for txn in transactions:
        # Get the inventory and span associated with the underlying.
        underlying, _ = options.GetUnderlying(txn.symbol)
        assert underlying
        optinv = unders[underlying][txn.symbol]
        if txn.txnType == 'TRADE':
            # Turn quantity into a signed value.
            quantity = txn.quantity

            if txn.effect == 'OPENING':
                mlist = optinv.match(txn, matchids)
                assert not mlist

            else:
                # Handle exiting a position.
                assert txn.effect == 'CLOSING'
                mlist = optinv.match(txn, matchids)
                assert mlist, "No match against: {}".format(pformat(txn))
                matched.extend(mlist)

        elif txn.txnType == 'EXPIRATION':
            # Handle options expiration.
            mlist = optinv.expire(txn, matchids)
            #assert mlist
            matched.extend(mlist)

    # Recreate a new list of transactions, including all that were matched and
    # the open ones.
    split_transactions = sorted([txn for match in matched for txn in match],
                                key=lambda t: t.date)

    for invdict in unders.values():
        for optinv in invdict.values():
            for txn in optinv.positions():
                split_transactions.append(
                    txn._replace(isOpen=True))
    del transactions

    # Print out matches.
    if 0:
        for match in matched:
            assert len({txn.matchId for txn in match}) == 1
            print(petl.wrap([m._replace(extra='') for m in match]).lookallstr())
        raise SystemExit

    def UniqueId(txn):
        return "{}-{}".format(txn.transactionId, txn.matchId)

    # Move this above and create the graph directly.
    graph = nx.Graph()
    for txn in split_transactions:
        graph.add_node(UniqueId(txn), type='txn', txn=txn)

    # Link together matches by their transaction ids.
    for txn in split_transactions:
        if txn.orderId:
            graph.add_node(txn.transactionId, type='origTxn')
            graph.add_edge(UniqueId(txn), txn.transactionId)

    # Link together opening transactions positions by order ids.
    if by_order:
        for txn in split_transactions:
            if txn.orderId:
                graph.add_node(txn.orderId, type='order')
                graph.add_edge(UniqueId(txn), txn.orderId)

    # Link together opening transactions positions by matches.
    if by_matching:
        for txn in split_transactions:
            if txn.matchId:
                graph.add_node(txn.matchId, type='match')
                graph.add_edge(UniqueId(txn), txn.matchId)

    # Link together opening transactions positions by time/spans.
    if by_overlap:
        changes = collections.defaultdict(list)
        for txn in split_transactions:
            underlying, _ = options.GetUnderlying(txn.symbol)
            changes[underlying].append(txn)

        span_counter = iter(itertools.count())
        for underlying, txnlist in changes.items():
            opened = 0
            for txn in sorted(txnlist, key=lambda t: t.date):
                if opened == 0:
                    spanId = "span{}".format(next(span_counter))
                    graph.add_node(spanId, type='span')
                change = 1 if txn.effect == 'OPENING' else -1
                opened += change
                graph.add_edge(UniqueId(txn), spanId)

    # Process each connected component to an individual trade.
    # Note: This includes rolls if they were carried one as a single order.
    trades = []
    for cc in nx.connected_components(graph):
        trade = []
        for nid in cc:
            node = graph.nodes[nid]
            if node['type'] == 'txn':
                trade.append(node['txn'])
        trade.sort(key=lambda txn: (txn.date, txn.transactionId))
        trades.append(trade)

    return trades


def Credits(row: Record) -> Decimal:
    return -row.quantity * row.price * CSIZE


def RenderTradeTable(trade: List[Txn], file=None):
    """Render a trade table to the givne file object."""

    pr = print
    if file:
        pr = functools.partial(print, file=file)

    trade = list(trade)
    trade.insert(0, Txn._fields)
    table = (petl.wrap(trade)
             .cutout('extra')
             .addfield('credits', Credits)
             .convert('credits', float))

    tclosed = table.select(lambda v: not v.isOpen)
    credits_closed = sum(tclosed.values('credits'))

    tactive = table.select(lambda v: v.isOpen)
    credits_active = sum(tactive.values('credits'))

    pos_mtm = sum(tactive
                  .select(lambda r: r.txnType == 'MARK')
                  .values('credits'))

    under, _ = options.GetUnderlying(trade[1].symbol)
    pr(tclosed.lookallstr())
    pr(credits_closed)
    pr()
    pr(tactive.lookallstr())
    pr(credits_active)
    pr()
    pr("Position MTM: {}".format(pos_mtm))
    pr("Chain Value: {}".format(credits_closed + credits_active))
    pr()


# TODO(blais): Also implement the boolean flags for matching.
# TODO(blais): Write unit tests.

# TODO(blais): Make sure not just date is taken into account, but also time,
# when span matching. An intra-day reset is valuable.

# TODO(blais): Really do build that price improvement automated algorithm that
# runs over time...
