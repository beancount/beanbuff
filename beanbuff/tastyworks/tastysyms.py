"""Parsing symbols from Tastyworks to a common normalized symbology called `BeanSyms`."""

import datetime
import re
from typing import Optional
from decimal import Decimal

from beanbuff.data import futures
from beanbuff.data import beansym


def ParseSymbol(symbol: str, itype: Optional[str]) -> beansym.Instrument:
    """Parse a symbol from the Tastyworks platforms."""
    if not symbol:
        return None
    # Futures options always start with a period.
    inst = None
    if itype == 'Future Option' or itype is None and symbol.startswith("./"):
        inst = _ParseFuturesOptionSymbol(symbol)
    # Futures always start with a slash.
    elif itype == 'Future' or itype is None and symbol.startswith("/"):
        inst = _ParseFuturesSymbol(symbol)
    # Then we have options, with a space.
    elif itype == 'Equity Option' or itype is None and ' ' in symbol:
        inst = _ParseEquityOptionSymbol(symbol)
    # And finally, just equities.
    elif itype == 'Equity' or itype is not None:
        inst = beansym.Instrument(underlying=symbol)
    else:
        raise ValueError(f"Unknown instrument type: {itype}")
    return inst


# Divisor for TW strike prices.
_STRIKE_PRICE_DIVISOR = Decimal('1000')


def _ParseEquityOptionSymbol(symbol: str) -> beansym.Instrument:
    # e.g. 'TLRY  210416C00075000' for equity option;
    return beansym.Instrument(
        underlying=symbol[0:6].rstrip(),
        expiration=datetime.date(int(symbol[6:8]), int(symbol[8:10]), int(symbol[10:12])),
        putcall=symbol[12],
        strike=Decimal(symbol[13:21]) / _STRIKE_PRICE_DIVISOR,
        multiplier=futures.OPTION_CONTRACT_SIZE)


_FUTSYM = "([A-Z0-9]+)([FGHJKMNQUVXZ])([0-9])"


def _ParseFuturesSymbol(symbol: str) -> beansym.Instrument:
    match = re.match(f"/{_FUTSYM}", symbol)
    assert match
    underlying, fmonth, fyear = match.groups()
    underlying = f"/{underlying}"
    decade = datetime.date.today().year % 100 // 10
    multiplier = futures.MULTIPLIERS.get(underlying, 1)
    return beansym.Instrument(
        underlying=underlying,
        calendar=f"{fmonth}{decade}{fyear}",
        multiplier=multiplier)


def _ParseFuturesOptionSymbol(symbol: str) -> beansym.Instrument:
    # e.g., "./6JM1 JPUK1 210507P0.009" for futures option.
    match = re.match(fr"\.(/[^ ]+) +{_FUTSYM} +(\d{{6}})([CP])([0-9.]+)", symbol)

    inst = _ParseFuturesSymbol(match.group(1))
    optcontract, optfmonth, optfyear = match.group(2,3,4)
    optdecade = datetime.date.today().year % 100 // 10
    optcalendar = f"{optfmonth}{optdecade}{optfyear}"

    expistr = match.group(5)
    expiration = datetime.date(int(expistr[0:2]), int(expistr[2:4]), int(expistr[4:6]))
    putcall = match.group(6)
    strike = Decimal(match.group(7))

    return inst._replace(optcontract=optcontract,
                         optcalendar=optcalendar,
                         expiration=expiration,
                         putcall=putcall,
                         strike=strike)
