"""Common code to process and validate transactions logs."""

from decimal import Decimal
from typing import Callable, Tuple
import datetime
import functools

from johnny.base import chains
from beanbuff.data import match
from johnny.base import instrument
from johnny.base.etl import Record, Table, petl


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]


# Transaction table field names.
FIELDS = [
    # Event info
    'account', 'transaction_id', 'datetime', 'rowtype', 'order_id',

    # Instrument info
    'symbol',

    # Balance info
    'effect', 'instruction', 'quantity', 'price', 'cost', 'commissions', 'fees',

    # Descriptive info
    'description',
]


class ValidationError(Exception):
    """Conformance for transactions table. Check your importer."""


def MakeParser(parser_fn: GetFn) -> ParserFn:
    """Make a parser function, including matches and chains."""

    @functools.wraps(parser_fn)
    def parser(filename: str) -> Table:
        output = parser_fn(filename)
        transactions = output[0]

        # Expand the instrument fields, as they are needed by the match and
        # chains modules.
        transactions = instrument.Expand(transactions, 'symbol')
        transactions = match.Match(transactions)
        transactions = chains.Group(transactions)
        transactions = instrument.Shrink(transactions)

        for rec in transactions.records():
            try:
                ValidateTransactionRecord(rec)
            except Exception as exc:
                raise ValidationError("Invalid validation on row {}".format(repr(rec))) from exc

        return transactions

    return parser


def IsZoneAware(d: datetime.datetime) -> bool:
    """Return true if the time is timezone aware."""
    return (d.tzinfo is not None and
            d.tzinfo.utcoffset(d) is not None)


# Valid row types.
ROW_TYPES = {'Trade', 'Expire', 'Mark'}

# Valid effect types. The empty string is used to indicate "unknown".
EFFECT = {'OPENING', 'CLOSING', '?'}

# Valid instructions.
INSTRUCTION = {'BUY', 'SELL'}


def ValidateTransactionRecord(r: Record):
    """Validate the transactions log for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    assert r.account and isinstance(r.account, str)
    assert r.transaction_id and isinstance(r.transaction_id, str)
    assert isinstance(r.datetime, datetime.datetime)
    assert not IsZoneAware(r.datetime)
    assert r.rowtype in ROW_TYPES
    assert r.order_id is None or (isinstance(r.order_id, str) and r.order_id)

    assert r.effect in EFFECT
    assert r.instruction in INSTRUCTION

    # Check the normalized symbol.
    assert r.symbol and isinstance(r.symbol, str)
    # TODO(blais): Parse the symbol to ensure it's right.
    ## assert instrument.Parse(r.symbol)

    # A quantity of 'None' is allowed if the logs don't include the expiration
    # quantity, and is filled in automatically by matching code.
    assert r.quantity is None or isinstance(r.quantity, Decimal)
    assert isinstance(r.price, Decimal)
    assert isinstance(r.cost, Decimal)
    assert isinstance(r.commissions, Decimal)
    assert isinstance(r.fees, Decimal)

    assert isinstance(r.description, str)
