"""Common code to process and validate transactions logs."""

import functools
from typing import Callable, Tuple

from beanbuff.data import chains
from beanbuff.data import match
from johnny.base.etl import Table


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]


def MakeParser(parser_fn: GetFn) -> ParserFn:
    """Make a parser function, including matches and chains."""

    @functools.wraps(parser_fn)
    def parser(filename: str) -> Table:
        output = parser_fn(filename)
        transactions = output[0]
        transactions = match.Match(transactions)
        return chains.Group(transactions)

    return parser
