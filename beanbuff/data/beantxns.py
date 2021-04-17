"""Conversion to and from Beancount transactions."""

from typing import List, NamedTuple

from beanbuff.data.rowtxns import Txn

from beancount.core import data
from beancount.core.account import Account


class Config(NamedTuple):

    # The cash currency.
    currency: str

    # Account names for cash, outrights (stocks and futures), options,
    # commission, fees and P/L.
    cash: Account
    outright: Account
    options: Account
    commissions: Account
    fees: Account
    pnl: Account

    # Formatting strings for ids.
    transaction_id: str
    order_id: str


def CreateTransactions(txns: List[Txn], config: Config) -> data.Entries:
    """Convert a list of Txn objects to Beancount Transaction."""

    for txn in txns:
        pass # print(txn)
    # TODO(blais): Do this.
