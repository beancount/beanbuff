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


# TODO(blais): Also implement the boolean flags for matching.
# TODO(blais): Write unit tests.

# TODO(blais): Make sure not just date is taken into account, but also time,
# when span matching. An intra-day reset is valuable.
"""

__author__ = 'Martin Blais <blais@furius.ca>'


from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, NamedTuple, Union, Set, Optional, Iterator, Tuple
import argparse
import hashlib
import functools
import collections
import pprint
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
from johnny.base.etl import petl, Table, Record, PrintGroups

import ameritrade as td
from ameritrade import options
from ameritrade import utils


def Group(transactions: Table,
          by_match=True,
          by_order=True,
          by_time=True) -> Table:
    """Aggregate transaction rows by options chain.

    Args:
      transactions: A normalized transactions log with a 'match' column.
      by_match: A flag, indicating that matching transactions should be chained.
      by_order: A flag, indicating that transactions from the same order should be chained.
      by_time: A flag, indicating that transactions overlapping over time should be chained.
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.
    """

    # Create a graph to link together related transactions.
    graph = nx.Graph()

    for rec in transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Link together by order id.
        if by_order:
            if rec.order_id:
                graph.add_node(rec.order_id, type='order')
                graph.add_edge(rec.transaction_id, rec.order_id)

        # Link together by match id.
        if by_match:
            if rec.match_id:
                graph.add_node(rec.match_id, type='match')
                graph.add_edge(rec.transaction_id, rec.match_id)

    # Link together matches that overlap in underlying and time.
    linked_matches = _LinkMatches(transactions)
    for match_id1, match_id2 in linked_matches:
        graph.add_edge(match_id1, match_id2)

    # Process each connected component to an individual trade.
    # Note: This includes rolls if they were carried one as a single order.
    chain_map = {}
    for cc in nx.connected_components(graph):
        chain_txns = []
        for transaction_id in cc:
            node = graph.nodes[transaction_id]
            if node['type'] == 'txn':
                chain_txns.append(node['rec'])

        chain_id = ChainName(chain_txns)
        for rec in chain_txns:
            chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: chain_map[r.transaction_id]))


def _LinkMatches(transactions: Table) -> List[Tuple[str, str]]:
    """Return pairs of linked matches."""

    # Gather min and max time for each trade match into a changelist.
    spans = []
    def GatherMatchSpans(grouper):
        min_datetime = datetime.datetime(2100, 1, 1)
        max_datetime = datetime.datetime(1970, 1, 1)
        for rec in sorted(list(grouper), key=lambda r: r.datetime):
            min_datetime = min(min_datetime, rec.datetime)
            max_datetime = max(max_datetime, rec.datetime)
        spans.append((min_datetime, rec.underlying, rec.match_id))
        spans.append((max_datetime, rec.underlying, rec.match_id))
        return 0
    list(transactions.aggregate(('underlying', 'match_id'), GatherMatchSpans)
         .records())

    # Process the spans in the order of time and allocate a new span id whenever
    # there's a gap without a position/match within one underlying.
    under_map = {underlying: set() for _, underlying, __ in spans}
    linked_matches = []
    for dt, underlying, match_id in sorted(spans):
        # Update the set of active matches, removing or adding.
        active_set = under_map[underlying]
        if match_id in active_set:
            active_set.remove(match_id)
        else:
            if active_set:
                # Link the current match-id to any other match id.
                other_match_id = next(iter(active_set))
                linked_matches.append((match_id, other_match_id))
            active_set.add(match_id)
    assert all(not active for active in under_map.values())

    return set(linked_matches)


def _CreateChainId(transaction_id: str, _: datetime.datetime) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "{}".format(md5.hexdigest())


def ChainName(txns: List[Record]) -> str:
    """Generate a unique chain name. This assumes 'account', 'mindate' and
    'underlying' columns."""

    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    mindate = min(rec.datetime for rec in txns)
    any_txn = txns[0]
    return ".".join([any_txn.account,
                     "{:%y%m%d_%H%M%S}".format(mindate),
                     any_txn.underlying.lstrip('/')])
