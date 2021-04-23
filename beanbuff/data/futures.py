"""Load information about futures contracts, in particular, the multipliers.
"""

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
