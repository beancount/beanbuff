#!/usr/bin/env python3
"""Web application for all the files.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Iterator, Iterable, Set, NamedTuple

from functools import partial
from os import path
import os
import threading
import re
import io

import click
import flask

from beanbuff.data import consolidated
from beanbuff.data.etl import petl, Table, Record, WrapRecords


approot = path.dirname(__file__)
app = flask.Flask(
    'buff',
    static_folder=path.join(approot, 'static'),
    template_folder=path.join(approot, 'templates'))


class State(NamedTuple):
    """Application state."""
    transactions: Table
    positions: Table
    chains: Table


def initialize():
    directory = os.getenv("HOME")
    fileordirs: str = [path.join(directory, fn)
                       for fn in os.listdir(directory)
                       if re.fullmatch(r'.*\.csv$', fn)]
    ledger: str = os.getenv("L")

    global STATE
    with _STATE_LOCK:
        if STATE is None:
            transactions, positions, chains = consolidated.ConsolidateChains(
                fileordirs, ledger)
            STATE = State(transactions, positions, chains)
    return STATE

STATE = None
_STATE_LOCK = threading.Lock()
app.before_first_request(initialize)


def ToHtmlString(table: Table, cls: str):
    sink = petl.MemorySource()
    table.tohtml(sink)
    html = sink.getvalue().decode('utf8')
    html = re.sub("class='petl'", f"class='display compact cell-border' id='{cls}'", html)
    return html


def GetNavigation() -> Dict[str, str]:
    """Get navigation bar."""
    return {
        'chains': flask.url_for('chains'),
        'transactions': flask.url_for('transactions'),
        'positions': flask.url_for('positions'),
        'risk': flask.url_for('risk'),
    }


def AddUrl(endpoint: str, kwdarg: str, value: Any) -> str:
    kwds = {kwdarg: value}
    return '<a href={}>{}</a>'.format(flask.url_for(endpoint, **kwds), value)


@app.route('/')
def home():
    return flask.redirect(flask.url_for('chains'))


@app.route('/chains')
def chains():
    table = (STATE.chains
             .convert('chain_id', partial(AddUrl, 'chain', 'chain_id')))
    return flask.render_template(
        'chains.html',
        table=ToHtmlString(table, 'chains'),
        **GetNavigation())


@app.route('/chain/<chain_id>')
def chain(chain_id: str):
    txns = (STATE.transactions
            .selecteq('chain_id', chain_id))

    clean_txns = (txns
                  .sort(['datetime', 'strike'])
                  .cut('datetime', 'description', 'cost'))

    svg = io.StringIO()
    if 0:
        prev_time = None
        for rec in clean_txns.records():
            if prev_time != rec.datetime:
                print(file=svg)
            print(rec, file=svg)
            prev_time = rec.datetime
    else:
        print('''
            <svg width="100" height="100">
              <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
            </svg>
        ''', file=svg)

    return flask.render_template(
        'chain.html',
        history=svg.getvalue(),
        table=ToHtmlString(txns, 'chain'),
        chain_id=chain_id,
        **GetNavigation())


@app.route('/transactions')
def transactions():
    table = (STATE.transactions
             .convert('chain_id', lambda v: flask.url_for('chain', chain_id=v)))
    return flask.render_template(
        'transactions.html',
        table=ToHtmlString(table, 'transactions'),
        **GetNavigation())


@app.route('/positions')
def positions():
    return flask.render_template(
        'positions.html',
        table=ToHtmlString(STATE.positions, 'positions'),
        **GetNavigation())


@app.route('/risk')
def risk():
    ## TODO(blais):
    return flask.render_template(
        'risk.html',
        **GetNavigation())


@app.route('/stats')
def stats():
    ## TODO(blais):
    return flask.render_template(
        'stats.html',
        **GetNavigation())


@app.route('/monitor')
def monitor():
    ## TODO(blais):
    return flask.render_template(
        'monitor.html',
        **GetNavigation())
