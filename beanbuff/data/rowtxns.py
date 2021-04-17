"""A trading transaction as a single table row.

This module defines a common format for expressing transactions involving
commodities in trading account, as a single row. Those transactions do not have
the full generality of a Beancount transaction, but offer a simplified model
which allows for the analysis of large trades. Think of this of a database of
your trading account statement reflecting everything but the transfers,
regulatory fees and misc. other transactions that aren't trades.

These row transactions can be converted to Beancount transactions and a subset
of Beancount transactions can be converted back into those.

You can use this data format as output for the importers from the trading
accounts.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
import datetime
from enum import Enum
from typing import Any, NamedTuple, Optional


class TxnType(Enum):
    """The tranasction type."""
    TRADE = 'TRADE'
    EXPIRATION = 'EXPIRATION'
    MARK = 'MARK'


class Instruction(Enum):
    """The instruction."""
    BUY = 'BUY'
    SELL = 'SELL'


class Effect(Enum):
    """The position effect."""
    OPENING = 'OPENING'
    CLOSING = 'CLOSING'


class Txn(NamedTuple):
    """A trading transaction object."""

    # The date and time at which the transaction occurred. This is distinct from
    # the settlement date (which is not provided by this data structure).
    timestamp: datetime.datetime

    # A unique transaction id by which we can identify this transaction. This is
    # essential in order to deduplicate previously imported transactions and is
    # usually available.
    transaction_id: str

    # The order id used for the transaction, if there was an order. This is used
    # to join together multiple transactions that were issued jointly, e.g. a
    # spread, an iron condor, a pairs trade, etc. Expirations don't have orders,
    # so will remain unset normally.
    order_id: Optional[str]

    # The id linking closing transactions to their corresponding opening ones.
    # This is normally not provided by the importers, and is filled in later by
    # analysis code.
    #
    # Note the inherent conflict in the 1:1 relationship here: a single
    # transactions may close multiple opening ones and vice-versa. In order to
    # make this a 1:1 match, we may have to split one or both of the
    # opening/closing sides. TODO(blais): Review this.
    match_id: Optional[str]

    # Trade, chain or strategy id linking together related transactions over
    # time. For instance, selling a strangle, then closing one side, and rolling
    # the other side, and then closing, could be considered a single chain of
    # events. As for the match id, this is normally empty and filled in later on
    # by analysis code.
    trade_id: Optional[str]

    # Whether this is a trade, an expiration, or a mark-to-market synthetic
    # close (used to value currency positions).
    txn_type: TxnType

    # The type of transaction, buy, sell. If this is an expiration, this can be
    # left unset.
    #
    # Like for the position effect, the value of this field can be inferred for
    # expirations but the state of the inventories will be required in ordered
    # to synthesize the right side.
    instruction: Optional[Instruction]

    # Whether the transaction is opening or closing, if it is known.
    #
    # If it is not known, state-based code providing and updating the state of
    # inventories is required to sort out whether this will cause an increase or
    # decrease of the position automatically. Ideally, if you have the sign,
    # include it here.
    effect: Optional[Effect]

    # The symbol for the underlying instrument. This may be an equity, equity
    # option, futures, futures option or spot currency pair.
    #
    # TODO(blais): Turn this to 'underyling' and add detail columns for options.
    symbol: str

    # The currency that the instrument is quoted in.
    currency: str

    # The quantity bought or sold. This number should always be positive. The
    # 'instruction' field will provide the sign.
    quantity: Decimal

    # The multiplier from the quantity. This can be left unset for an implicit
    # value of 1. For equity options, it should be set to 100, and for futures
    # contracts, set to whatever the multiplier for the contract is. (These
    # values are static and technically they could be joined dynamically from
    # another table, but for the purpose of keeping a simple table and ensuring
    # against historical adjustments in contract multipliers we include it
    # here.)
    multiplier: Optional[Decimal]

    # The price, in the quote currency, at which the instrument transacted.
    price: Decimal

    # The total amount in commissions, which are the fees paid to the broker for
    # service.
    commissions: Decimal

    # The total amount paid for exchange and other regulatory institution fees.
    fees: Decimal

    # A textual description to attach to the transaction, if one is available.
    # This can be used to convert to Beancount transactions to provide
    # meaningful narration.
    description: Optional[str]

    # Any object you want to attach to this transaction. This can be the
    # original transaction object in the source data, or None. Normally this
    # comes in unset.
    extra: Any
