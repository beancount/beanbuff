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
