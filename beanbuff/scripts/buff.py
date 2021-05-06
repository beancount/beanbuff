#!/usr/bin/env python3
"""Web application for all the files.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Iterator, Iterable, Set, NamedTuple

from os import path
import os
import threading
import re

import click
import flask

from beanbuff.data import consolidated
from beanbuff.data.etl import petl, Table, Record, WrapRecords


templates = path.join(path.dirname(__file__), 'templates')
assert path.isdir(templates)
app = flask.Flask('buff', template_folder=templates)


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
    return {'chains': flask.url_for('chains'),
            'transactions': flask.url_for('transactions'),
            'positions': flask.url_for('positions')}


@app.route('/')
def home():
    return flask.redirect(flask.url_for('chains'))


@app.route('/chains')
def chains():
    return flask.render_template(
        'chains.html',
        table=ToHtmlString(STATE.chains, 'chains'),
        **GetNavigation())


@app.route('/chain/<chain_id>')
def chain(chain_id: str):
    txns = (STATE.transactions
            .selecteq('chain_id', chain_id))
    return flask.render_template(
        'chain.html',
        table=ToHtmlString(txns, 'chain'),
        chain_id=chain_id,
        **GetNavigation())


@app.route('/transactions')
def transactions():
    return flask.render_template(
        'transactions.html',
        table=ToHtmlString(STATE.transactions, 'transactions'),
        **GetNavigation())


@app.route('/positions')
def positions():
    return flask.render_template(
        'positions.html',
        table=ToHtmlString(STATE.positions, 'positions'),
        **GetNavigation())


# @click.group(cls=flask.FlaskGroup, create_app=create_app)
# def cli():
#     """Management script."""
#
# if __name__ == '__main__':
#     cli()
