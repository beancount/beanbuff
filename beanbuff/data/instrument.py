# Rename this to instrument.py.

"""Normalized symbols."""

import datetime
import re
from decimal import Decimal
from typing import List, NamedTuple, Optional

from johnny.base import futures
from johnny.base.etl import Table


# TODO(blais): Normalize all this to include the calendar months in the
# underlying and match the fields in instrument.md.

# TODO(blais): Set the expiration datetime for Future Option instruments to the
# end of the corresponding calendar month. It's better than nothing, and you can
# use it to synthesize expirations where missing.


# TODO(blais): Add methods to create each of the subtypes of instruments, with
# validation.
#
# TODO(blais): Add accessor for instrument type (e.g., "Future Option").
#
# TODO(blais): Fold the calendar months into the underlyings and provide methods
# to extract them instead.
#
# TODO(blais): What about the subtype, e.g. (European) (Physical), etc.? That
# is currently lost.


# A representation of an option.
class Instrument(NamedTuple):
    """An instrument broken down by its component fields.
    See instrument.md for details.
    """

    # The name of the underlying instrument, stock or futures. For futures, this
    # includes the leading slash and the expiration month code (e.g., 'Z21').
    # Example '/CLZ21'. Note that the decade is included as well.
    underlying: str





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
    putcall: Optional[str] = None

    # For futures and options on futures contracts, the multiplier for the
    # instrument.
    multiplier: Optional[int] = None


    def __str__(self):
        """Convert an instrument to a string code."""
        return ToString(self)

    @property
    def instype(self) -> str:
        """Return the instrument type."""
        if self.underlying.startswith('/'):
            return 'FutureOption' if self.expiration is not None else 'Future'
        else:
            return 'EquityOption' if self.expiration is not None else 'Equity'

    def is_future(self) -> bool:
        return self.underlying.startswith('/')

    @property
    def dated_underlying(self) -> str:
        """Return the expiration code."""
        return GetDatedUnderlying(self)

    @property
    def expcode(self) -> str:
        """Return the expiration code."""
        return GetExpirationCode(self)


def FromColumns(
        underlying: str,
        expiration: datetime.date,
        expcode: str,
        putcall: str,
        strike: Decimal,
        multiplier: Optional[Decimal]) -> Instrument:
    """Build an Instrument from column values."""
    match = re.match('(/.*)([FGHJKMNQUVXZ]2\d)', underlying)
    if match:
        _, calendar = match.groups()
    else:
        calendar = None

    optcontract, optcalendar = None, None
    if expcode:
        match = re.match('(.*)([FGHJKMNQUVXZ]2\d)', expcode)
        if match:
            optcontract, optcalendar = match.groups()

    # TODO(blais): Normalize to 'CALL' or 'PUT'
    side = putcall[0] if putcall else None

    # Infer the multiplier if it is not provided.
    if multiplier is None:
        if calendar is None:
            if expiration is not None:
                multiplier = futures.OPTION_CONTRACT_SIZE
            else:
                multiplier = 1
        else:
            multiplier = futures.MULTIPLIERS[underlying[:-3]]

    return Instrument(underlying, optcontract, optcalendar,
                      expiration, strike, side, multiplier)


def FromString(symbol: str) -> Instrument:
    """Build an instrument object from the symbol string."""

    # Match options.
    match = re.match(r'(.*)_(?:(\d{6})|(.*))_([CP])(.*)', symbol)
    if match:
        underlying, expi_str, expcode, putcall, strike_str = match.groups()
        expiration = (datetime.datetime.strptime(expi_str, '%y%m%d').date()
                      if expi_str
                      else None)
        strike = Decimal(strike_str)
    else:
        expiration, expcode, putcall, strike = None, None, None, None
        underlying = symbol

    return FromColumns(underlying,
                       expiration,
                       expcode,
                       putcall,
                       strike,
                       None)


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
            return "{}_{}{}_{:%y%m%d}_{}{}".format(
                inst.underlying,
                inst.optcontract, inst.optcalendar,
                inst.expiration, inst.putcall, inst.strike)
        else:
            # Without date
            return "{}_{}{}_{}{}".format(
                inst.underlying,
                inst.optcontract, inst.optcalendar,
                inst.putcall, inst.strike)

    elif inst.is_future():
        # Future
        return inst.underlying

    else:
        # Equity option
        if inst.expiration is not None:
            return "{}_{:%y%m%d}_{}{}".format(
                inst.underlying, inst.expiration, inst.putcall, inst.strike)
        else:
            return inst.underlying


def GetDatedUnderlying(inst: Instrument) -> str:
    """Return the underlying name with the month code, if present."""
    return inst.underlying


def GetExpirationCode(inst: Instrument) -> str:
    """Return the futures option expiration code."""
    if inst.optcontract:
        assert inst.optcalendar, inst
        return "{}{}".format(inst.optcontract, inst.optcalendar)
    return ''


def GetContractName(symbol: str) -> str:
    """Return the underlying root without the futures calendar expiration, e.g. '/CL'."""
    underlying = symbol.split('_')[0]
    if underlying.startswith('/'):
        match = re.match('(.*)([FGHJKMNQUVXZ]2\d)', underlying)
        assert match, string
        return match.group(1)
    else:
        return underlying


def Expand(table: Table, fieldname: str) -> Table:
    """Expand the symbol name into its component fields."""
    return (table
            .addfield('_instrument', lambda r: FromString(getattr(r, fieldname)))
            .addfield('instype', lambda r: r._instrument.instype)
            .addfield('underlying', lambda r: r._instrument.underlying)
            .addfield('expiration', lambda r: r._instrument.expiration)
            .addfield('expcode', lambda r: r._instrument.expcode)
            .addfield('putcall', lambda r: r._instrument.putcall)
            .addfield('strike', lambda r: r._instrument.strike)
            .addfield('multiplier', lambda r: r._instrument.multiplier)
            .cutout('_instrument'))


def Shrink(table: Table) -> Table:
    """Remove the component fields of the instrument."""
    return (table
            .cutout('instype', 'underlying', 'expiration', 'expcode',
                    'putcall', 'strike', 'multiplier'))
