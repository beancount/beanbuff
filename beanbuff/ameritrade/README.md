# Ameritrade Importers and Reconciliation

This code parses and reconciles data that can be downloaded from the
Ameritrade and/or Think-or-Swim platform.

## Goals

We want to

- Export all non-trade information to Beancount directives for bookkeeping.

- Export a table-based trade log of all trade-related information
  (see
  https://docs.google.com/document/d/1H0UDD1cKenraIMe40PbdMgnqJdeqI6yKv0og51mXk-0/).
  This table should be convertible to Beancount directives.

Furthermore, we want to be able to

- Produce as good a trade log and positions monitor as possible from any
  information available intra-day, e.g., update right after a trade if needed,
  as early as possible.

- Pull in and merge more detailed information which becomes available from the
  API and at settlement.


## Sources of Data

The TOS exports are all partial and difficult to process (for historical
reasons, probably) and it causes multiples headaches in reconciliation and
position monitoring.

There are different sources of historical transaction and cash statement data
available:

- The "Account Statement" CSV download (from the Monitor tab). This is a single
  CSV file download ("Export to File...") that contains many tables, but in
  particular, it contains,

  * Cash balances for (a) the main cash statement, (b) the futures account and
    (c) the forex account.
  * Trade history, which includes data over equity, equity options, futures, and
    futures options.
  * Order history.
  * Summary of positions, by type.

  Note that by default the Futures Statement is merge with the Cash Balances,
  which is innacurate. The TD backend accounts for futures in a separate account
  (due to separate margin treatment, accounting and probably regulations) and
  you can call the desk to have a flag turned on that will break out the futures
  statement separately. I believe the FOREX statement is handled like that by
  default. The conversion scripts assume you have done that to your account (and
  processes it as such).

  This statement can be produced during the day to provide fresh data.

- The "API" (https://developer.tdameritrade.com/) provides a more detailed view
  of transactions and one that is structured in JSON format and very consistent.
  This would be our favorite source of data, if not for two major flaws: it
  won't provide the data until a day or two after (it is delayed) and it does
  not include any of the futures statement action.

  * The API also provides decent position monitoring, but it completely ignores
    the futures positions. The indices use a separate symbology (e.g. "$SPX.X"
    instead of "SPX").
  * As mentioned, the production of that log during the day will only reflect
    data that occurred up to a day or two before.

### Unfortunate Quirks

The "Cash Balance" statement displays fees in the TOS UI, but the downloadable
file does not include them. However, we're able to back those out from the
differences in the balance column.

The "Account Trade History" does not contain fees. We can join this table with
the "Cash Balance" table, but this the latter merges multiple legs of a
transaction (e.g. IRON CONDOR) to a single line, all the fees are summed up as
one.

Oftentimes, the futures statement is just not showing the days you request. I
have no idea why. This looks like a bug.

The API, on the other hand, provides the fees on each of the legs but is only
available later on.

Finally, the log from the API loses information about the strategy used for
options combos. Using common order ids, it should be possible to reconstruct
those.

NOTE: Our approach will be as follows: We will join all these tables as they
become available and cross check them against each other in order to produce a
final log with as much information as possible.


### Identifiers

A number of identifiers are present:

- "Ref #": These appear unique, but it's unclear how they tie in with the orders
  and transactions. They appear to be matchini some, but not all, or the order
  ids in trading table.

- "Order ID": These link together multiple transactions that were issued as one
  (e.g.a  spread).

  Unfortunately, for linked orders (e.g. pairs) the ids are distinct, but
  consecutive. We could attempt to reconcile and link together consecutive ids
  where the date/time are the same.

- "Transaction IDs": These appear globally unique and are only available from
  the API's transaction log.


## Non-Trade Data

Non-trading data includes cash transfers, misc. regulatory fees, sweeps,
corrections and other non-trading events. These are imported from

- The "Cash Balances" tables for equities and equity options.
- The "Futures Statements" for the futures account.
- The "Forex Statements" for the FOREX account (this is currently not supported)>

and produced as Beancount transactions. They are not regular, and require the
flexibility of multiple accounts to book properly.

The three-letter row "Type" is used to select which rows are going to be in the
trade log and which aren't. We include everything that's a trade, options
expirations and assignments (but not other "receive-and-deliver" messages) and
dividends.

Note that for the "Cash Balances" table, the API produces more detailed
information about fees and is used to produce Beancount entries. However, data
from the "Futures Statement" is not available through the API and the "Account
Statement" CSV is used to produce its entries.


## Trade Data - Reconciliation and Log Production

In contrast, trading data is much more regular and can be expressed as a table.
In order to produce an accurate trade log as early as possible, various sources
of data are reconciled together:

- The "Account Trade History" table is pulled from the "Account Statement" CSV
  download.

- It is joined against the "Cash Balance" table in order to pull in fee
  information from it intra-day. These rows are moved to the trade log, and the
  "Cash Balance" table remains with non-trading events.

- It is joined against the "Futures Statements" table in order to pull in fee
  information from it intra-day. These rows are moved to the trade log, and the
  "Futures Statements" table remains with non-trading events.

This merged trade log is stored.
Later on,

- The API is used to pull transaction data after the fact and is joined against
  the trade log produced from the "Account Statement" tables.


## Position Monitoring

There are two sources of data for positions:

- The "Equities", "Options", "Futures" and "Futures Options" table from the
  "Account Statement" CSV download.

- The API via its "GetAccounts()" endpoint. Unfortunately, this endpoint does
  not provide any positions held in the futures account, so futures contracts
  nor futures options.
