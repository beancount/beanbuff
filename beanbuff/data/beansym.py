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
    # futures, this is the expitation of the option, not of the underlying; this
    # should be compatible with the 'optcalendar' field.
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

    @property
    def dated_underlying(self) -> str:
        """Return the expiration code."""
        return GetDatedUnderlying(self)

    @property
    def expcode(self) -> str:
        """Return the expiration code."""
        return GetExpirationCode(self)


def ToString(inst: Instrument) -> str:
    """Convert an instrument to a string code."""

    if inst.optcontract:
        # Future Option
        #
        # Note: For options on futures, the correct expiration date isn't always
        # available (e.g. from TOS). We ignore it for that reason, the date is
        # implicit in the option code. It's not very precise, but better to be
        # consistent.
        ## inst.expiration is not None:
        if False:
            # With date
            return "{}{}_{}{}_{:%y%m%d}_{}{}".format(
                inst.underlying, inst.calendar,
                inst.optcontract, inst.optcalendar,
                inst.expiration, inst.side, inst.strike)
        else:
            # Without date
            return "{}{}_{}{}_{}{}".format(
                inst.underlying, inst.calendar,
                inst.optcontract, inst.optcalendar,
                inst.side, inst.strike)

    elif inst.calendar:
        # Future
        return "{}{}".format(inst.underlying, inst.calendar)

    else:
        # Equity option
        if inst.expiration is not None:
            return "{}_{:%y%m%d}_{}{}".format(
                inst.underlying, inst.expiration, inst.side, inst.strike)
        else:
            return inst.underlying


def GetDatedUnderlying(inst: Instrument) -> str:
    """Return the underlying name with the month code, if present."""
    if inst.calendar:
        return "{}{}".format(inst.underlying, inst.calendar)
    return inst.underlying


def GetExpirationCode(inst: Instrument) -> str:
    """Return the futures option expiration code."""
    if inst.optcontract:
        assert inst.optcalendar, inst
        return "{}{}".format(inst.optcontract, inst.optcalendar)
    return ''
