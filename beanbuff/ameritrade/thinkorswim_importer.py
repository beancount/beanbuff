"""Think-or-Swim "Account Statement" CSV detail importer.

Instructions:
- Start TOS
- Go to the "Monitor" tab
- Select the "Account Statement" page
- Select the desired time period
- Right on the rightmost hamburger menu and select "Export to File..."

This module implements a pretty tight reconciliation from the AccountStatement
export to CSV, joining and verifying the equities cash and futures cash
statements with the trade history.

Caveats:
- Transaction IDs are missing can have to be joined in later from the API.
"""

import csv
import re
import itertools
import datetime
import collections
import typing
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from decimal import Decimal
from functools import partial
from itertools import chain

from dateutil import parser
import petl
petl.config.look_style = 'minimal'
petl.config.failonerror = True

from beancount.core.number import ZERO
from beancount.core.number import ONE
from beancount.core.amount import Amount
from beancount.core.inventory import Inventory
from beancount.core import data
from beancount.core import position
from beancount.core import inventory
from beancount.core import flags
from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core.number import MISSING
from beancount.utils import csv_utils
from beancount.utils.snoop import save

from beangulp import testing
from beangulp.importers.mixins import config
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier

from beanbuff.data import beantxns
from beanbuff.data import futures
from beanbuff.data import beansym
from beanbuff.data.etl import petl, Table, Record



debug = False
Config = Any


class Importer(identifier.IdentifyMixin, filing.FilingMixin, config.ConfigMixin):

    REQUIRED_CONFIG = {
        'currency'            : 'Currency used for cash account',
        'asset_cash'          : 'Cash account',
        'asset_money_market'  : 'Money market account associated with this account',
        'asset_forex'         : 'Retail foreign exchange trading account',
        'futures_contracts'   : 'Root account holding contracts',
        'futures_options'     : 'Subaccount containing options',
        'futures_margin'      : 'Margin used, in dollars',
        'futures_cash'        : 'Cash account for futures only',
        'futures_pnl'         : 'Profit/loss on futures contracts',
        'futures_miscfees'    : 'Miscellanious fees',
        'futures_commissions' : 'Commissions',
        'asset_position'      : 'Account for all positions, with {symbol} format',
        'option_position'     : 'Account for options positions, with {symbol} format',
        'fees'                : 'Fees',
        'commission'          : 'Commissions',
        'interest'            : 'Interest income',
        'dividend_nontax'     : 'Non-taxable dividend income, with {symbol} format',
        'dividend'            : 'Taxable dividend income, with {symbol} format',
        'adjustment'          : 'Free / unknown / miscellaneous adjustment account',
        'pnl'                 : 'Capital Gains/Losses',
        'transfer'            : 'Other account for inter-bank transfers',
        'third_party'         : 'Other account for third-party transfers (wires)',
        'opening'             : 'Opening balances account, used to make transfer when you opt-in',
    }

    matchers = [
        ('mime', r'text/(plain|csv)')
    ]

    def extract(self, file):
        """Import a CSV file from Think-or-Swim."""
        print()
        print()

        ## TODO(blais): Continue here.
        ##thinkorswim_transactions.GetTransactions(file.name)

        bconfig = beantxns.Config(
            self.config['currency'],
            self.config['futures_cash'],
            self.config['futures_contracts'],
            self.config['futures_options'],
            self.config['futures_commissions'],
            self.config['futures_miscfees'],
            self.config['pnl'],
            'td-{}',
            'order-{}')
        futures_entries = beantxns.CreateTransactions(futures_txns, bconfig)
        return futures_entries


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Ameritrade:Main', config={
        'currency'            : 'USD',
        'asset_cash'          : 'Assets:US:Ameritrade:Main:Cash',
        'asset_money_market'  : 'Assets:US:Ameritrade:Main:MMDA1',
        'asset_position'      : 'Assets:US:Ameritrade:Main:{symbol}',
        'option_position'     : 'Assets:US:Ameritrade:Main:Options',
        'asset_forex'         : 'Assets:US:Ameritrade:Forex',
        'futures_contracts'   : 'Assets:US:Ameritrade:Futures:Contracts',
        'futures_options'     : 'Assets:US:Ameritrade:Futures:Options',
        'futures_margin'      : 'Assets:US:Ameritrade:Futures:Margin',
        'futures_cash'        : 'Assets:US:Ameritrade:Futures:Cash',
        'futures_pnl'         : 'Income:US:Ameritrade:Futures:PnL',
        'futures_miscfees'    : 'Expenses:Financial:Fees',
        'futures_commissions' : 'Expenses:Financial:Commissions',
        'fees'                : 'Expenses:Financial:Fees',
        'commission'          : 'Expenses:Financial:Commissions',
        'interest'            : 'Income:US:Ameritrade:Main:Interest',
        'dividend_nontax'     : 'Income:US:Ameritrade:Main:{symbol}:Dividend:NoTax',
        'dividend'            : 'Income:US:Ameritrade:Main:{symbol}:Dividend',
        'adjustment'          : 'Income:US:Ameritrade:Main:Misc',
        'pnl'                 : 'Income:US:Ameritrade:Main:PnL',
        'transfer'            : 'Assets:US:TD:Checking',
        'third_party'         : 'Assets:US:Other:Cash',
        'opening'             : 'Equity:Opening-Balances',
    })
    testing.main(importer)
