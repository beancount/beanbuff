"""Vanguard CSV download import.

This module parses the downloadable CSV files from Vanguard, both for the
retirement and brokerage accounts.
"""

from decimal import Decimal
from os import path
from typing import Dict, List, Optional
import csv
import datetime
import io
import itertools
import logging
import pprint
import re

import petl
from dateutil import parser

from beancount.core import account
from beancount.core import account_types
from beancount.core import amount
from beancount.core import data
from beancount.core import flags
from beancount.core import position
from beancount.core.amount import Amount
from beancount.core.data import EMPTY_SET
from beancount.core.number import D
from beancount.core.number import ZERO

from beangulp import csv_utils
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


acctypes = account_types.DEFAULT_ACCOUNT_TYPES


def parse_date(string):
    return datetime.datetime.strptime(string, "%m/%d/%Y").date()


# --------------------------------------------------------------------------------

BROKERAGE_POSITIONS = (
    "Account Number,Investment Name,Symbol,Shares,Share Price,Total Value,"
)


def _parse_brokerage_positions(filename, config, reader):
    latest_trade_date = _get_newest_trade_date(filename)
    date = latest_trade_date + datetime.timedelta(days=1)
    # date = datetime.datetime.fromtimestamp(path.getctime(filename)).date()

    entries = []
    for index, row in enumerate(reader):
        meta = data.new_metadata(filename, 1000)
        currency = symbol = row["Symbol"]
        if symbol == config["mmf_currency"]:
            symbol = "Cash"
            currency = config["cash_currency"]
        acc = config["assets_roth_ira"].format(symbol=symbol)
        units = amount.Amount(D(row["Shares"]), currency)
        entry = data.Balance(meta, date, acc, units, None, None)
        entries.append(entry)
    return entries


def _get_newest_trade_date(filename):
    """Find the most recent trade date anywhere in the file.

    This is used to infer a balance date, because the file does not contain
    any hint of a production date.
    """
    max_date = datetime.date(1970, 1, 1)
    with open(filename, encoding="iso-8859-1") as infile:
        for section in csv_utils.iter_sections(infile):
            header = next(section)
            if "Trade Date" in header:
                reader = csv.DictReader(itertools.chain([header], section))
                for row in reader:
                    date = parse_date(row["Trade Date"])
                    if date > max_date:
                        max_date = date
    return max_date


# --------------------------------------------------------------------------------

BROKERAGE_TRANSACTIONS = (
    "Account Number,Trade Date,Settlement Date,Transaction Type,"
    "Transaction Description,Investment Name,Symbol,Shares,Share Price,"
    "Principal Amount,Commission Fees,Net Amount,Accrued Interest,Account Type,"
)


def _parse_brokerage_transactions(filename, config, reader):
    currency = config["cash_currency"]

    entries = []
    for row in reversed(list(reader)):
        # Clean up the row, parse types.
        for col in {
            "Accrued Interest",
            "Commission Fees",
            "Net Amount",
            "Principal Amount",
            "Share Price",
            "Shares",
        }:
            row[col] = Decimal(row[col])
        for col in {"Settlement Date", "Trade Date"}:
            row[col] = parse_date(row[col])
        ttype = row["Transaction Type"]
        tdesc = row["Transaction Description"]

        # Render a nice narration.
        strings = [ttype]
        if tdesc and tdesc != ttype:
            strings.append(": {}".format(tdesc))
        if row["Symbol"]:
            strings.append(" ({})".format(row["Symbol"]))
        narration = "".join(strings)

        # Create a transaction.
        txn = data.Transaction(
            {},
            row["Trade Date"],
            flags.FLAG_OKAY,
            None,
            narration,
            EMPTY_SET,
            EMPTY_SET,
            [],
        )
        if row["Settlement Date"] != row["Trade Date"]:
            txn.meta["settlement_date"] = row["Settlement Date"]

        # Ignore two types of sweeps.
        if ttype in {"Sweep in", "Sweep out"}:
            assert row["Commission Fees"] == ZERO
            assert row["Accrued Interest"] == ZERO

        elif ttype == "Reinvestment":
            assert tdesc == "Dividend Reinvestment"
            assert row["Commission Fees"] == ZERO
            assert row["Accrued Interest"] == ZERO

        elif ttype == "Dividend":
            assert tdesc == "Dividend Received"
            assert row["Commission Fees"] == ZERO
            assert row["Accrued Interest"] == ZERO

            # If there is no symbol, this is interest accrued.
            units = Amount(row["Net Amount"], currency)
            if row["Symbol"]:
                acc_income = config["income_dividend"].format(symbol=row["Symbol"])
            else:
                acc_income = config["income_interest"]
            acc_cash = config["assets_roth_ira"].format(symbol="Cash")

            txn.postings.append(
                data.Posting(acc_income, -units, None, None, None, None)
            )
            txn.postings.append(data.Posting(acc_cash, units, None, None, None, None))
            entries.append(txn)

        elif ttype == "Buy":
            assert tdesc == "Buy"
            assert row["Accrued Interest"] == ZERO

            # Stock posting.
            acc_stock = config["assets_roth_ira"].format(symbol=row["Symbol"])
            units = Amount(row["Shares"], row["Symbol"])
            cost = position.Cost(row["Share Price"], currency, None, None)
            txn.postings.append(data.Posting(acc_stock, units, cost, None, None, None))

            # Cash postings.
            acc_cash = config["assets_roth_ira"].format(symbol="Cash")
            units = Amount(row["Net Amount"], currency)
            txn.postings.append(data.Posting(acc_cash, units, None, None, None, None))

            # Fees posting.
            if row["Commission Fees"] != ZERO:
                units = Amount(row["Commission Fees"], currency)
                txn.postings.append(
                    data.Posting(
                        config["expenses_commissions"], units, None, None, None, None
                    )
                )

            entries.append(txn)

        elif ttype == "Sell":
            assert tdesc == "Sell"
            assert row["Accrued Interest"] == ZERO

            # Stock posting.
            acc_stock = config["assets_roth_ira"].format(symbol=row["Symbol"])
            units = Amount(row["Shares"], row["Symbol"])
            cost = position.Cost(None, currency, None, None)
            price = Amount(row["Share Price"], currency)
            txn.postings.append(data.Posting(acc_stock, units, cost, price, None, None))

            # Cash postings.
            acc_cash = config["assets_roth_ira"].format(symbol="Cash")
            units = Amount(row["Net Amount"], currency)
            txn.postings.append(data.Posting(acc_cash, units, None, None, None, None))

            # Fees posting.
            if row["Commission Fees"] != ZERO:
                units = Amount(row["Commission Fees"], currency)
                txn.postings.append(
                    data.Posting(
                        config["expenses_commissions"], units, None, None, None, None
                    )
                )

            # P/L posting.
            txn.postings.append(
                data.Posting(config["income_pnl"], None, None, None, None, None)
            )

            entries.append(txn)

        elif (
            ttype == "Sell to open"
            or ttype == "Buy to close"
            or ttype == "Transfer (outgoing)"
        ):
            assert row["Accrued Interest"] == ZERO
            logging.critical("TODO(blais): Support not implemented for {}.".format(row))
            entries.append(txn)

        elif ttype == "Transfer (outgoing)":
            logging.critical("TODO(blais): Support not implemented for {}.".format(row))
            entries.append(txn)

        elif ttype in {"Capital gain (LT)", "Capital gain (ST)"}:
            assert re.match(r"(Long|Short)-Term Capital Gains Distribution", tdesc)
            assert row["Commission Fees"] == ZERO
            assert row["Accrued Interest"] == ZERO

            # P/L posting.
            units = Amount(row["Net Amount"], currency)
            txn.postings.append(
                data.Posting(config["income_pnl"], -units, None, None, None, None)
            )

            # Cash postings.
            acc_cash = config["assets_roth_ira"].format(symbol="Cash")
            txn.postings.append(data.Posting(acc_cash, units, None, None, None, None))

            entries.append(txn)

        elif ttype == "Rollover (incoming)":
            # Transfer posting.
            acc_xfer = config["assets_transfer"]
            units = Amount(row["Net Amount"], currency)
            txn.postings.append(data.Posting(acc_xfer, -units, None, None, None, None))

            # Cash posting.
            acc_cash = config["assets_roth_ira"].format(symbol="Cash")
            txn.postings.append(data.Posting(acc_cash, units, None, None, None, None))

            entries.append(txn)

        else:
            raise TypeError(
                "Handler for row not implemented: {}".format(pprint.pformat(row))
            )

    for index, entry in enumerate(entries):
        entry.meta["filename"] = filename
        entry.meta["lineno"] = index

    return entries


# --------------------------------------------------------------------------------

RETIREMENT_POSITIONS = "Plan Number,Plan Name,Fund Name,Shares,Price,Total Value,"


def _parse_retirement_positions(filename, config, reader):
    for row in reader:
        pass


# --------------------------------------------------------------------------------

RETIREMENT_TRANSACTIONS = (
    "Account Number,Trade Date,Run Date,Transaction Activity,"
    "Transaction Description,Investment Name,Share Price,Transaction Shares,"
    "Dollar Amount,"
)


def _parse_retirement_transactions(filename, config, reader):
    for row in reader:
        pass


# --------------------------------------------------------------------------------

HANDLERS = {
    BROKERAGE_POSITIONS: _parse_brokerage_positions,
    BROKERAGE_TRANSACTIONS: _parse_brokerage_transactions,
    RETIREMENT_POSITIONS: _parse_retirement_positions,
    RETIREMENT_TRANSACTIONS: _parse_retirement_transactions,
}


CONFIG = {
    "cash_currency": "Currency used for cash account",
    "mmf_currency": "Money-Market Fund Currency",
    "assets_401k_pretax": "Root of pre-tax subaccounts",
    "assets_401k_match": "Root of employer match subaccounts",
    "assets_401k_aftertax": "Root of after-tax subaccounts",
    "assets_roth_ira": "Root of Roth IRA subaccounts",
    "assets_transfer": "External transfer account",
    "income_dividend": "Root of dividend income accounts",
    "income_interest": "Interest income account",
    "income_pnl": "P/L income account",
    "expenses_commissions": "Commissions expenses account",
    "rounding_error": "Rounding error account",
}


class Importer(beangulp.Importer):
    def __init__(self, filing: str, config: Dict[str, str]):
        self._account = filing
        self.config = config
        utils.validate_accounts(CONFIG, config)

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        return utils.search_file_regexp(
            filepath, *HANDLERS.keys(), encoding="iso-8859-1"
        )

    def date(self, filepath: str) -> Optional[datetime.date]:
        return None

    def filename(self, filepath: str) -> Optional[str]:
        return "vanguard.{}".format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        # Parse each of the sections through handlers.
        new_entries = []
        with open(filepath, encoding="iso-8859-1") as infile:
            for section in csv_utils.iter_sections(infile):
                header = next(section)
                try:
                    handler = HANDLERS[header.rstrip()]
                except KeyError:
                    raise ValueError("Invalid header: {}".format(header))
                reader = csv.DictReader(itertools.chain([header], section))
                entries = handler(filepath, self.config, reader)
                if entries:
                    new_entries.extend(entries)
        return new_entries


def extract_tables(filepath: str) -> List[petl.Table]:
    # Parse each of the sections through handlers.
    tables = []
    with open(filepath, encoding="iso-8859-1") as infile:
        for section in csv_utils.iter_sections(infile):
            header = next(section)
            try:
                handler = HANDLERS[header.rstrip()]
            except KeyError:
                raise ValueError("Invalid header: {}".format(header))
            buf = io.StringIO()
            buf.write(header)
            [buf.write(line) for line in section]
            tables.append(
                petl.fromcsv(petl.MemorySource(buf.getvalue().encode("utf8")))
            )
    if not len(tables) == 2:
        raise ValueError(
            "Invalid CSV file with more than the expected number of sections."
        )
    instruments, transactions = tables

    transactions = transactions.convert(
        ["Share Price", "Transaction Shares", "Dollar Amount"], D
    ).convert(["Trade Date", "Run Date"], lambda x: parser.parse(x).date())

    return [instruments, transactions]


if __name__ == "__main__":
    importer = Importer(
        filing="Assets:US:Vanguard",
        config={
            "cash_currency": "USD",
            "mmf_currency": "VMFXX",
            "assets_401k_pretax": "Assets:US:Vanguard:Retire:PreTax401:{symbol}",
            "assets_401k_match": "Assets:US:Vanguard:Retire:Match401k:{symbol}",
            "assets_401k_aftertax": "Assets:US:Vanguard:Retire:AfterTax:{symbol}",
            "assets_roth_ira": "Assets:US:Vanguard:RothIRA:{symbol}",
            "assets_transfer": "Assets:US:Vanguard:Retire:AfterTax:Cash",
            "income_dividend": "Income:US:Vanguard:RothIRA:{symbol}:Dividend",
            "income_interest": "Income:US:Vanguard:RothIRA:Interest",
            "income_pnl": "Income:US:Vanguard:RothIRA:PnL",
            "expenses_commissions": "Expenses:Financial:Commissions",
            "rounding_error": "Equity:RoundingError",
        },
    )
    testing.main(importer)
