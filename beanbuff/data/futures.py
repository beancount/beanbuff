"""Load information about futures contracts, in particular, the multipliers.
"""

from typing import Tuple


# Standard equity option contract size.
OPTION_CONTRACT_SIZE = 100

MULTIPLIERS = {
    # Indices: S&P 500
    '/ES'  : 50,
    '/MES' : 5,
    'SPX'  : 100,

    # Indices: Nasdaq 100
    '/NQ'  : 20,
    '/MNQ' : 2,
    'NDX'  : 100,

    # Indices: Russell 2000
    '/RTY' : 50,
    '/M2K' : 5,
    'RUT'  : 100,

    # Indices: Dow Jones
    '/YM'  : 5,
    '/MYM' : 0.5,
    'DJI'  : 100,

    # FX
    '/6E'  : 125_000,
    '/6J'  : 12_500_000,
    '/6A'  : 100_000,
    '/6C'  : 100_000,

    # Energy
    '/CL'  : 1000,
    '/NG'  : 10_000,

    # Metals
    '/GC'  : 100,
    '/SI'  : 5000,
    '/HG'  : 25000,

    # Rates
    '/ZQ'  : 4167,
    '/GE'  : 2500,
    '/ZT'  : 2000,
    '/ZF'  : 1000,
    '/ZN'  : 1000,
    '/ZB'  : 1000,

    # Agricultural
    '/ZC'  : 50,
    '/ZS'  : 50,
    '/ZW'  : 50,

    # Livestock
    '/HE'  : 400,
    '/LE'  : 400,
}


# This is a mapping of (option-product-code, month-code) to
# (futures-product-code, month-code). Options are offered on a monthly basis,
# but the underlying futures contract isn't necessarily offered for every month
# (depends on seasonality sometimes), so the underlying is sometimes for the
# same month (and the options expire a few days ahead of the futures) or for the
# subsequent month (in which case multiple months are applicable to the same
# underlying).
#
# CME has definitions on this, like this: "/SI: Monthly contracts listed for 3
# consecutive months and any Jan, Mar, May, and Sep in the nearest 23 months and
# any Jul and Dec in the nearest 60 months."
# https://www.cmegroup.com/trading/metals/precious/silver_contractSpecs_options.html
#
# We need to eventually encode all those rules as logic, as some input files
# (notably, from TOS) sometimes only produce the options code and in order to
# produce a normalized symbol we need both.

# NOTE(blais): Temporary monster hack, based on my own file.
# Update as needed.

_TEMPORARY_MAPPING = {
    ('/SO', 'M'): ('/SI', 'N'),
    ('/OG', 'N'): ('/GC', 'Q'),
    ('/EUU', 'M'): ('/6E', 'M'),
    ('/OZC', 'N'): ('/ZC', 'N'),
    ('/OZS', 'N'): ('/ZS', 'N'),
}

def GetUnderlyingMonth(optcontract: str, optmonth: str) -> Tuple[str, str]:
    """Given the contract code and its month (e.g., '/SOM'), return the underlying
    future and its month ('/SIN'). The reason this function exists is that not
    all the months are avaiable as underlyings. This depends on the particulars
    of each futures contract, and the details depend on cyclicality /
    availability / seasonality of the product.
    """
    return _TEMPORARY_MAPPING[(optcontract, optmonth)]
