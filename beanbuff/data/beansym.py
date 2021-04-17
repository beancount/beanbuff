"""Normalized symbols."""

import datetime
from decimal import Decimal
from typing import List, NamedTuple, Optional


# A representation of an option.
class Instrument(NamedTuple):
    # The name of the underlying instrument, stock or futures. For futures, this
    # includes the leading slash and does not include the expiration month code
    # (e.g., 'Z1'). Example '/CL'.
    underlying: str

    # For futures, the contract expiration month code, including the decade,
    # e.g., 'Z21'. This can be converted to a date for the expiration.
    calendar: Optional[str] = None

    # For options on futures, the particular options contract code, e.g. on /CL,
    # this could be 'LOM'.
    optcontract: Optional[str] = None

    # For options on futures, the contract expiration month code for the options
    # itself, e.g., 'Z21'.
    optcalendar: Optional[str] = None

    # For options, the expiration date for the options contract. For options on
    # futures, this should be compatible with the 'fo_calendar' field.
    expiration: Optional[datetime.date] = None

    # For options, the strike price.
    strike: Optional[Decimal] = None

    # For options, the side is represented by the letter 'C' or 'P'.
    side: Optional[str] = None

    # For futures and options on futures contracts, the multiplier for the
    # instrument.
    multiplier: Optional[int] = None


    def __str__(self):
        """Convert an instrument to a string code."""
        return ToString(self)


def ToString(inst: Instrument) -> str:
    """Convert an instrument to a string code."""

    if inst.optcontract:
        return "{}{}_{}{}_{:%y%m%d}_{}{}".format(
            inst.underlying, inst.calendar,
            inst.optcontract, inst.optcalendar,
            inst.expiration, inst.side, inst.strike)

    elif inst.calendar:
        return "{}{}".format(inst.underlying, inst.calendar)

    else:
        return "{}_{:%y%m%d}_{}{}".format(
            inst.underlying, inst.expiration, inst.side, inst.strike)
