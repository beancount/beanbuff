"""CSV importer for Alerus 401(k) plan.

Go to

  My Balance > History Browse; Go > Save History

then manually go to

  My Balacne > Investment

and save balances for holdings.
"""
__author__ = "Martin Blais <blais@furius.ca>"

from os import path
from typing import Optional
import datetime
import sys

import petl

from beancount.core import account
from beancount.core import amount
from beancount.core import data
from beancount.core import flags
from beancount.core import position

from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


class Importer(beangulp.Importer):
    def __init__(self, filing):
        self._account = filing

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        header = "Date,Time,Sequence,Investment,Source,Activity,Ticker,Cusip,Price,Shares,Value"
        return utils.is_mimetype(filepath, "text/csv") and utils.search_file_regexp(
            filepath, header
        )

    def date(self, filepath: str) -> Optional[datetime.date]:
        _, max_date = (
            petl.fromcsv(filepath).convert("Date", utils.parse_date).limits("Date")
        )
        return max_date + datetime.timedelta(days=1)

    def filename(self, filepath: str) -> Optional[str]:
        return "alerus.{}".format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        currency = "USD"

        def create_transaction(r: petl.Record) -> data.Transaction:
            stock_account = account.join(self._account, r["Ticker"])
            cash_account = account.join(self._account, "Cash")
            meta = data.new_metadata(f"<{__file__}>".format, 0)
            meta["time"] = r["Time"]
            txn = data.Transaction(
                meta,
                r["Date"],
                flags.FLAG_OKAY,
                None,
                "{Activity} ({Ticker}, {Investment}, {Cusip}) from {Source}".format(
                    **dict(zip(r.flds, r))
                ),
                set(),
                set(),
                [
                    data.Posting(
                        stock_account,
                        amount.Amount(r["Shares"], r["Ticker"]),
                        position.Cost(r["Price"], currency, None, None),
                        None,
                        None,
                        None,
                    ),
                    data.Posting(
                        cash_account,
                        amount.Amount(-r["Value"], currency),
                        None,
                        None,
                        None,
                        None,
                    ),
                ],
            )
            return txn

        table = (
            petl.fromcsv(filepath)
            .convert("Date", utils.parse_date)
            .convert("Sequence", int)
            .convert(["Price", "Shares", "Value"], utils.parse_amount)
            .sort(["Date", "Sequence"])
            .addfield("txn", create_transaction)
        )

        return table.values("txn")


if __name__ == "__main__":
    importer = Importer("Assets:US:Alerus")
    testing.main(importer)
