# PositionsTable

- `account`: A unique identifier for the account number. This implicitly defines
  the brokerage. This can be used if a file contains information about multiple
  accounts.

TODO(blais): Rename 'pnl' to 'pnl_open', it's more clear and in line with platforms.


TODO(blais): Do more...


account  instype        symbol                 quantity  price       mark      cost      net_liq   pnl       pnl_day
x1887    Future Option  /6AM21_ADUM21_C0.795   -1        0.00240     -0.00130  240.00    -130.00   110.00    110.00
x1887    Future Option  /6AM21_ADUM21_C0.84    1         -0.00030    0.00007   -30.00    6.76      -23.24    -3.24
x1887    Future Option  /6AM21_ADUM21_P0.7     1         -0.00050    0.00020   -50.00    20.00     -30.00    5.00
x1887    Future Option  /6AM21_ADUM21_P0.745   -1        0.00250     -0.00155  250.00    -155.00   95.00     -45.00
x1887    Future Option  /6CM21_CAUM21_C0.825   -2        0.00110     -0.00230  220.00    -460.00   -240.00   40.00
