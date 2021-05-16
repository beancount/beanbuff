# Instrument Fields

The following fields are optional, and can be entirely inferred from the
`symbol` field alone. These can be expanded from the symbol when specific
processing of its components is required, e.g., when the `strike` field is
needed on its own.

- `instype: str`: The instrument type, an enum with the following possible
  values:

  * `Equity`
  * `Equity Option`
  * `Future`
  * `Future Option`

- `underlying: str`: The underlying instrument, with normalized name. (e.g., if
  this is a futures, it will always include the decade.)

- `expiration: Optional[datetime.date]`: The expiration date of an option. If
  this is an option on a future, this may not be present and need be inferred
  separately (insert it if you have it).

- `expcode: Optional[str]`: The expiration date of an option. If this is an
  option on a future, the corresponding option expiration code, e.g. `LOM21` for
  `/CLM21`.

- `putcall: Optional[str]`: If an option, `CALL` or `PUT`

- `strike: Optional[str]`: The strike price of an option (Decimal).

- `multiplier: int`: A multiplier for the contract, i.e., the contract size.

  This is a multiplier for the quantity. For equities, this is 1. For equity
  options, it should be set to 100. For futures contracts, set to whatever the
  multiplier for the contract is. (These values are static and technically are
  inferred automatically from the underlying and instrumen ttype.

The currency that the instrument is quoted in is not included; we assume the US
dollar is the quoting currency so far.
