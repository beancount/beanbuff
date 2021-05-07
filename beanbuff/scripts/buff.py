#!/usr/bin/env python3
"""Web application for all the files.
"""

from decimal import Decimal
from functools import partial
from os import path
from typing import Any, Callable, Dict, List, Optional, Tuple, Iterator, Iterable, Set, NamedTuple
import io
import itertools
import os
import re
import threading

import click
import flask

from beanbuff.data import consolidated
from beanbuff.data.etl import petl, Table, Record, WrapRecords


ZERO = Decimal(0)


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


def ToHtmlString(table: Table, cls: str, ids: List[str] = None):
    sink = petl.MemorySource()
    table.tohtml(sink)
    html = sink.getvalue().decode('utf8')
    html = re.sub("class='petl'", f"class='display compact cell-border' id='{cls}'", html)
    if ids:
        iter_ids = itertools.chain([''], iter(ids))
        html = re.sub('<tr>', lambda _: '<tr id="{}">'.format(next(iter_ids)), html)
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
    ids = STATE.chains.values('chain_id')
    table = (STATE.chains
             .convert('chain_id', partial(AddUrl, 'chain', 'chain_id')))
    return flask.render_template(
        'chains.html',
        table=ToHtmlString(table, 'chains', ids),
        **GetNavigation())


@app.route('/chain/<chain_id>')
def chain(chain_id: str):
    txns = (STATE.transactions
            .selecteq('chain_id', chain_id))

    clean_txns = (txns
                  .sort(['datetime', 'strike'])
                  .cut('datetime', 'description', 'strike', 'cost'))

    print(clean_txns.lookallstr())

    strikes = {strike for strike in clean_txns.values('strike') if strike is not None}
    min_strike = min(strikes)
    max_strike = max(strikes)
    diff_strike = (max_strike - min_strike)
    if diff_strike == 0:
        diff_strike = 1
    min_x = 0
    max_x = 1000
    width = 1000

    svg = io.StringIO()
    pr = partial(print, file=svg)
    if 0:
        prev_time = None
        for rec in clean_txns.records():
            if prev_time != rec.datetime:
                print(file=svg)
            print(rec, file=svg)
            prev_time = rec.datetime
    else:
        pr(f'<svg viewBox="-150 0 1300 1500" xmlns="http://www.w3.org/2000/svg">')
        pr('<style>')
        pr('''
                .small { font-size: 7px; }
                .normal { font-size: 9px; }
        ''')


        #         /* Note that the color of the text is set with the    *
        #          * fill property, the color property is for HTML only */
        #         .Rrrrr { font: italic 40px serif; fill: red; }

        # pr('''
        #       <text x="20" y="35" class="small">My</text>
        #       <text x="40" y="35" class="heavy">cat</text>
        #       <text x="55" y="55" class="small">is</text>
        #       <text x="65" y="55" class="Rrrrr">Grumpy!</text>
        # ''')

        pr('</style>')

        pr(f'<line x1="0" y1="4" x2="1000" y2="4" style="stroke:#cccccc;stroke-width:0.5" />')
        for strike in sorted(strikes):
            x = int((strike - min_strike) / diff_strike * width)
            pr(f'<line x1="{x}" y1="2" x2="{x}" y2="6" style="stroke:#333333;stroke-width:0.5" />')
            pr(f'<text text-anchor="middle" x="{x}" y="12" class="small">{strike}</text>')
        pr()

        y = 20
        prev_time = None
        for r in clean_txns.sort('datetime').records():
            if prev_time is not None and prev_time != r.datetime:
                y += 30
            # print(rec, file=svg)
            prev_time = r.datetime

            x = int((r.strike - min_strike) / diff_strike * width)
            pr(f'<text text-anchor="middle" x="{x}" y="{y}" class="normal">{r.description}</text>')
            y += 12

        pr('</svg>')

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


@app.route('/summarize')
def summarize():
    # Filter down the list of chains.
    selected_chain_ids = flask.request.args.get('chain_ids')
    if selected_chain_ids:
        selected_chain_ids = selected_chain_ids.split(',')

    chains = (STATE.chains
              .selectin('chain_id', selected_chain_ids)
              .addfield('pnl', lambda r: r.accr + (r.net_liq or ZERO))
              .cut('underlying', 'mindate', 'days', 'init', 'pnl'))

    # Add bottom line totals.
    totals = (chains
              .cut('init', 'pnl')
              .aggregate(None, {'init': ('init', sum),
                                'pnl': ('pnl', sum)})
              .addfield('underlying', '__TOTAL__'))
    chains = petl.cat(chains, totals)

    return flask.render_template(
        'summary.html',
        table=ToHtmlString(chains, 'summary'),
        **GetNavigation())
