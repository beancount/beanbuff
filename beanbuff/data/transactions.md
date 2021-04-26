# Transactions Log Table

A *normalized* transactions table contains the following columns and types.


## Information about the rows

- `account`: A unique identifier for the account number. This implicitly defines
  the brokerage. This can be used if a file contains information about multiple
  accounts.

- `transaction_id`: A unique transaction id. This can be given from the system
  or synthesized from a stable hash from the rows of an input file.

- `datetime`: A `datetime.datetime` instance, converted to local time and naive
  (no timezone).

- `rowtype`: An enum for the row, one of

  * `Trade` (a purchase or sale),
  * `Expiration` (an expiration),
  * `Mark` (a virtual sale).

  `MARK` is never inserted by the normalization code, that's something that is
  inserted by further processing code.

- `order_id`: If relevant, a unique id for the number.


## A description of the instrument itself

- `instype`: The instrument type, an enum with possible values:

  * `Equity`
  * `Equity Option`
  * `Future`
  * `Future Option`

- `underlying`: The underlying instrument.

- `expiration`: The expiration date of an option. If this is an option on a
  future, this may not be present and need be inferred separately (insert it if
  you have it).

- `expcode`: The expiration date of an option. If this is an option on a
  future, the corresponding option expiration code, e.g. `LOM21` for `/CLM21`.

- `side`: If an option, `CALL` or `PUT`

- `strike`: The strike price of an option (Decimal).

- `multiplier`: A multiplier for the contract, i.e., the contract size.


## Information affecting the balance

- `effect`: The effect on the position, either `OPENING` or `CLOSING`. For
  futures contracts, the is not known usually (but can be inferred later, from
  the initial positions).

- `instruction`: An enum, `BUY`, `SELL` or None (for expirations).

- `quantity`: A positive number for the quantity of items.

- `price`: The per-contract price for the instrument. Multply this by the
  `quantity` and the `multiplier` to get the `cost`.

- `cost`: The dollar amount of the position change minus commissions and fees.
  This is a signed number.

- `commissions`: The dollar amount of commissions charged. This is a signed
  number, usually negative.

- `fees`: The dollar amount of fees charged. This is a signed number, usually
  negative.


## Superfluous information

- `description`: An optional free-form description string describing the
  transaction. This is used for debugging and for rendering transactions in
  accounting systems.





TODO(blais): Write a routine which validates a normalized trade table.




## TODO(blais): Remove and replace by simple petl tables as per `transactions.md`.


# Appendix

TODO(blais): Merge in some of the descriptionsf from this in eventually.

```
class Txn(NamedTuple):
    """A trading transaction object."""

    # The date and time at which the transaction occurred. This is distinct from
    # the settlement date (which is not provided by this data structure).
    datetime: datetime.datetime

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
    rowtype: TxnType

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
    underlying: str

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

    # Instrument type.
    instype: str

```
