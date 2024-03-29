"""OFX file format importer for investment accounts.

Update on 2022-05-18: You have a choice of OFX/QFX file or CSV. Always select
both downloads, so that we can join them together:

- Only the OFX file contains the "source" field, which is necessary for proper
  account details.

- Only the CSV file contains the necessary detail to distinguish the different
  types of transfers in and out. See the definition of _DESCRIPTION_MAP below.

"""
from decimal import Decimal
from os import path
from typing import Dict, Optional
import datetime
import itertools
import pprint
import re
import logging

import bs4
import petl

from beancount.core import account
from beancount.core import amount
from beancount.core import data
from beancount.core import flags
from beancount.core import position
from beancount.core.data import EMPTY_SET
from beancount.core.number import D
from beancount.core.number import ZERO

from beanbuff.utils.ofx import parse_ofx_time
from beangulp import petl_utils
from beangulp import testing
from beangulp import utils
import beangulp


CONFIG = {
    "asset_account": (
        "Asset accounts " "(include {source} for subaccount, {security} for security)"
    ),
    "asset_balances": (
        "Root account for balances of a particular security "
        "(include {source} for subaccount, {security} for security)"
    ),
    "cash_account": (
        "Cash accounts " "(include {source} for subaccount, {security} for security)"
    ),
    "income_account": (
        "Income accounts "
        "(include {source} for subaccount, {security} for security, "
        "{type} for income type)"
    ),
    "income_match": (
        "Source account for match contributions "
        "(include {source} for subaccount, {security} for security)"
    ),
    "income_pnl": "P/L income account",
    "expenses_fees": "Fees expenses",
    "source_pretax": "Name of pre-tax subaccount",
    "source_aftertax": "Name of after-tax subaccount",
    "source_match": "Name of match contributions subaccount",
    "source_rollover": "Name of rollover subaccount",
    "source_othernonvest": "Name of other/non-vesting subaccount",
}


Q1 = Decimal("0.1")
Q2 = Decimal("0.01")
Q5 = Decimal("0.00001")


class Importer(beangulp.Importer):
    def __init__(self, filing: str, account_id: str, config: Dict[str, str]):
        self._account = filing
        self._account_id = account_id
        self.config = config
        utils.validate_accounts(CONFIG, config)

    def account(self, filepath: str) -> data.Account:
        return self._account

    def identify(self, filepath: str) -> bool:
        return (
            utils.is_mimetype(
                filepath, {"application/x-ofx", "application/vnd.intu.qfx"}
            )
            and utils.search_file_regexp(filepath, r"Vanguard", encoding="cp1252")
            and utils.search_file_regexp(
                filepath, r"<ACCTID>{}".format(self._account_id), encoding="cp1252"
            )
        )

    def date(self, filepath: str) -> Optional[datetime.date]:
        return None

    def filename(self, filepath: str) -> Optional[str]:
        return "vanguard.{}".format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        logging.info("Extracting from OFX/QFX")
        entries, transactions_ofx, securities = extract_from_ofx(
            filepath, self._account_id, self.config, flags.FLAG_OKAY
        )

        # Join in the securities info.
        transactions = petl.leftjoin(transactions_ofx, securities, "uniqueid").sort(
            "dttrade"
        )

        # Infer the activity code solely from the OFX input.
        transactions = infer_activity(transactions)

        # Trim up some of the fat.
        transactions = transactions.cutout(
            "uniqueid", "fitit", "incometype", "subacctsec", "postype", "fiid"
        )

        if 0:
            print()
            print(transactions.lookallstr())

        # Note: In July 2022, I made the parsing routine return *part of* the
        # OFX file contents as a table and joined both the CSV and OFX files to
        # see if the CSV was adding data that wasn't present in the OFX. It
        # turns out the transaction type is more explicit in the CVS file but
        # can be inferred solely from the OFX file, so decided we'd be better
        # using just the OFX file (better precision in the reported numbers).
        # The code is still here though. It could be an idea one day to return
        # only tables from the extract_from_ofx() and create the transactions in
        # a separate step. Keeping it as is for now.
        return entries


def infer_activity(transactions_ofx: petl.Table) -> petl.Table:
    # Create a mapping from the data we have in the OFX file only, so
    # that we never need use the CSV file if not available.
    description_map = petl.wrap(
        [
            ["tran", "tferaction", "memo", "activity", "description"],
            [
                "REINVEST",
                None,
                "Price as of date based on closing price",
                "ACT_REINVEST",
                "Dividends on Equity Investments",
            ],
            [
                "TRANSFER",
                "IN",
                "Price as of date based on closing price",
                "ACT_TRANSFER_IN",
                "Plan Initiated TransferIn",
            ],
            [
                "TRANSFER",
                "OUT",
                "Investment Expense",
                "ACT_FEE",
                "Fee (Investment Expense)",
            ],
            [
                "TRANSFER",
                "OUT",
                "Price as of date based on closing price",
                "ACT_TRANSFER_OUT",
                "Plan Initiated TransferOut",
            ],
        ]
    ).lookupone(["tran", "tferaction", "memo"])

    if 0:
        txntypes = transactions.cut(
            "tran",
            "tferaction",
            "memo",
            "Transaction Activity",
            "Transaction Description",
        ).distinct()
        print(txntypes.lookallstr())
        # tran      tferaction  memo                                     Transaction Activity  Transaction Description
        # REINVEST  None        Price as of date based on closing price  219004                Dividends on Equity Investments
        # TRANSFER  IN          Price as of date based on closing price  382007                Plan Initiated TransferIn
        # TRANSFER  OUT         Investment Expense                       245026                Fee
        # TRANSFER  OUT         Price as of date based on closing price  382021                Plan Initiated TransferOut

    def get_description(rec):
        memo = rec["memo"]
        if memo == "Price as of date based on closing price":
            memo = None
        description = rec["Transaction Description"]
        if memo:
            description = f"{description} ({memo})"
        return description

    return (
        transactions_ofx.addfield(
            "activity",
            lambda r: description_map[(r["tran"], r["tferaction"], r["memo"])][-2],
        )
        .addfield(
            "description",
            lambda r: description_map[(r["tran"], r["tferaction"], r["memo"])][-1],
        )
        .cutout("tran", "tferaction", "memo")
    )


def try_join_csv_file(transactions_ofx: petl.Table, filepath: str):
    # Look for an accompanying CSV file and join it if present to see if
    # the contents are in any way complementary to the OFX one (is it
    # worth downloading it and joining the fields? (Answer: No)).
    dirname, basename = path.split(filepath)
    csv_filepath = path.join(dirname, re.sub(r"\.qfx", ".csv", basename).lower())
    if path.exists(csv_filepath):
        logging.info("Extracting from CSV")
        from beanbuff.vanguard import vanguard_csv
        balances, transactions_csv = vanguard_csv.extract_tables(csv_filepath)
        transactions_both = join_csv_file(transactions_ofx, transactions_csv)
        print()
        print(transactions_both.lookallstr())


def join_csv_file(transactions: petl.Table, transactions_csv: petl.Table) -> petl.Table:
    """Read and join the CSV file with the OFX-derived table."""
    # for table in tables:
    #     print(table.lookallstr())
    # print()
    # print(transactions_csv.lookallstr())

    # Join the OFX and CSV contents based on some reliable common fields.
    transactions = petl.leftjoin(
        transactions.addfield(
            "key",
            lambda r: (
                r["dttrade"],
                r["dtsettle"],
                abs(r["unitprice"].quantize(Q2)),
                abs(r["units"].quantize(Q1)),
                abs(r["total"].quantize(Q2)),
            ),
        ),
        transactions_csv.addfield(
            "key",
            lambda r: (
                r["Trade Date"],
                r["Run Date"],
                abs(r["Share Price"].quantize(Q2)),
                abs(r["Transaction Shares"].quantize(Q1)),
                abs(r["Dollar Amount"].quantize(Q2)),
            ),
        ),
        key="key",
    ).cutout("key")

    # Validate some invariants.
    def assert_equal(value1, value2):
        equal = value1 == value2
        if not equal:
            raise ValueError("Values differ: '{}' != '{}'".format(value1, value2))
        return equal

    list(
        transactions.addfield(
            "_dttrade",
            lambda r: assert_equal(r["dttrade"], r["Trade Date"]),
        )
        .addfield(
            "_dtsettle",
            lambda r: assert_equal(r["dtsettle"], r["Run Date"]),
        )
        .addfield(
            "_total",
            lambda r: assert_equal(abs(r["total"]), abs(r["Dollar Amount"])),
        )
        .addfield(
            "_unitprice",
            lambda r: assert_equal(
                r["unitprice"].quantize(Q5), r["Share Price"].quantize(Q5)
            ),
        )
    )

    return transactions


def extract_from_ofx(
    filename: str, account_id: str, config: Dict[str, str], flag: str
) -> [data.Entries, petl.Table]:
    """Extract transaction info from the given OFX file into transactions for the
    given account. This function returns a list of entries possibly partially
    filled entries.
    """

    # Prepare mappings to accounts from the config provided.
    sources = {
        "PRETAX": config["source_pretax"],
        "MATCH": config["source_match"],
        "AFTERTAX": config["source_aftertax"],
        "ROLLOVER": config["source_rollover"],
        "OTHERNONVEST": config["source_othernonvest"],
    }

    def get_other_account(trantype, source, _incometype):
        if trantype == "BUYMF" and source == "MATCH":
            return config["income_match"].format(
                source="{source}", security="{security}"
            )
        elif trantype == "REINVEST":
            return config["income_account"].format(
                source="{source}", security="{security}", type="Dividend"
            )
        elif trantype == "TRANSFER":
            return config["expenses_fees"]
        else:
            return config["cash_account"].format(
                source="{source}", security="{security}"
            )

    new_entries = []

    # Parse the XML file.
    with open(filename, errors="ignore") as infile:
        soup = bs4.BeautifulSoup(infile, "lxml")

    # Get the description of securities used in this file.
    securities = get_securities(soup)
    if securities:
        securities_map = securities.recordlookupone("uniqueid")

    # For each statement.
    txn_counter = itertools.count()
    rows = []
    for stmtrs in soup.find_all(re.compile(".*stmtrs$")):
        # account_type = stmtrs.find('accttype').text.strip()
        # bank_id = stmtrs.find('bankid').text.strip()
        acctid = stmtrs.find("acctid").text.strip()
        if not re.match(account_id, acctid):
            continue

        # For each currency.
        for currency_node in stmtrs.find_all("curdef"):
            currency = currency_node.contents[0].strip()

            # Process all investment transaction lists.
            # Note: this was developed for Vanguard.
            for invtranlist in stmtrs.find_all(re.compile("(invtranlist)")):

                # Process stock transactions.
                for tran in invtranlist.find_all(
                    re.compile(
                        "(buymf|sellmf|reinvest|buystock|sellstock|buyopt|sellopt|transfer)"
                    )
                ):
                    # dtposted = parse_ofx_time(soup_get(tran, 'dtposted')).date()
                    date = parse_ofx_time(soup_get(tran, "dttrade")).date()

                    uniqueid = soup_get(tran, "uniqueid")
                    security = securities_map[uniqueid]["ticker"]

                    units = soup_get(tran, "units", D)
                    unitprice = soup_get(tran, "unitprice", D)
                    total = soup_get(tran, "total", D)
                    if total is None:
                        total = units * unitprice

                    fileloc = data.new_metadata(filename, next(txn_counter))
                    payee = None

                    trantype = tran.name.upper()
                    incometype = soup_get(tran, "incometype")
                    source = soup_get(tran, "inv401ksource")
                    memo = soup_get(tran, "memo")
                    narration = " - ".join(
                        filter(None, (trantype, incometype, source, memo))
                    )

                    if source is None:
                        msg = "Could not establish source for {} on {}: '''\n{}'''".format(
                            security, date, tran.prettify()
                        )
                        if tran.name in ("sellmf", "reinvest"):
                            # When we sell anything without a source, we
                            # assume it's always for an after-tax to roth
                            # conversion.
                            source = "AFTERTAX"
                        else:
                            raise ValueError(msg)

                    entry = data.Transaction(
                        fileloc, date, flag, payee, narration, EMPTY_SET, EMPTY_SET, []
                    )

                    # Create stock posting.
                    tferaction = soup_get(tran, "tferaction")
                    if tferaction == "OUT":
                        assert units < ZERO

                    rows.append(
                        {
                            "dttrade": parse_ofx_time(soup_get(tran, "dttrade")).date(),
                            "dtsettle": parse_ofx_time(
                                soup_get(tran, "dtsettle")
                            ).date(),
                            "uniqueid": uniqueid,
                            "fitit": soup_get(tran, "fitid"),
                            "units": units,
                            "unitprice": unitprice,
                            "total": total.quantize(Q2),
                            "tran": tran.name.upper(),
                            "incometype": incometype,
                            "source": source,
                            "memo": memo,
                            "tferaction": tferaction,
                            "subacctsec": soup_get(tran, "subacctsec"),
                            "postype": soup_get(tran, "postype"),
                        }
                    )

                    units_amount = amount.Amount(units, security)
                    cost = position.Cost(unitprice, currency, None, None)
                    account_sec = config["asset_account"].format(
                        source=sources[source], security=security
                    )
                    entry.postings.append(
                        data.Posting(account_sec, units_amount, cost, None, None, None)
                    )

                    # Compute total amount.
                    if tran.name == "transfer":
                        total = -total
                    elif tran.name == "buymf":
                        assert total is not None
                        assert abs(total + (units * unitprice)) < 0.005, abs(
                            total - (units * unitprice)
                        )
                    elif tran.name == "reinvest":
                        assert total is not None
                        assert abs(abs(total) - abs(units * unitprice)) < 0.005, abs(
                            abs(total) - abs(units * unitprice)
                        )
                        if not re.search("DIVIDEND REINVESTMENT", memo):
                            # This is going to get booked to a dividend leg; invert the sign.
                            total = -total

                    # Create cash posting.
                    account_template = get_other_account(trantype, source, incometype)
                    account_cash = account_template.format(
                        source=sources[source], security=security
                    )
                    entry.postings.append(
                        data.Posting(
                            account_cash,
                            amount.Amount(total, currency),
                            None,
                            None,
                            None,
                            None,
                        )
                    )

                    # On a sale, add a leg to absorb the P/L.
                    if tran.name == "sellmf":
                        account_pnl = config["income_pnl"]
                        entry.postings.append(
                            data.Posting(account_pnl, None, None, None, None, None)
                        )

                    new_entries.append(entry)

                # Process the cash account transactions.
                for tran in invtranlist.find_all(re.compile("(invbanktran)")):
                    date = dtposted = parse_ofx_time(soup_get(tran, "dtposted")).date()
                    number = soup_get(tran, "trnamt", D)
                    name = soup_get(tran, "name")
                    memo = soup_get(tran, "memo")
                    fitid = soup_get(tran, "fitid")
                    subacctfund = soup_get(tran, "subacctfund")

                    rows.append(
                        {
                            "date": date,
                            "dttrade": dtposted,
                            "trnamt": number,
                            "name": name,
                            "memo": memo,
                            "subacctfund": subacctfund,
                        }
                    )

                    assert (
                        subacctfund == "CASH"
                    )  # I don't know what the other transaction types could be.

                    fileloc = data.new_metadata(filename, next(txn_counter))

                    narration = "{} - {}".format(name, memo)
                    account_ = get_other_account("OTHER", "AFTERTAX", None).format(
                        source=sources[source]
                    )
                    entry = data.Transaction(
                        fileloc,
                        date,
                        flag,
                        None,
                        narration,
                        EMPTY_SET,
                        {fitid},
                        [
                            data.Posting(
                                account_,
                                amount.Amount(number, currency),
                                None,
                                None,
                                None,
                                None,
                            )
                        ],
                    )

                    new_entries.append(entry)

                # Process all positions, convert them to data.Balance directives.
                # Note: this was developed for Vanguard.
                for invposlist in stmtrs.find_all("invposlist"):
                    for invpos in invposlist.find_all("invpos"):
                        date = parse_ofx_time(
                            soup_get(invpos, "dtpriceasof")
                        ).date() + datetime.timedelta(days=1)

                        uniqueid = soup_get(invpos, "uniqueid")
                        security = securities_map[uniqueid]["ticker"]

                        units = soup_get(invpos, "units", D)
                        unitprice = soup_get(invpos, "unitprice", D)

                        fileloc = data.new_metadata(filename, next(txn_counter))
                        source = soup_get(invpos, "inv401ksource")
                        if source is None:
                            continue
                        account_ = config["asset_balances"].format(
                            source=sources[source], security=security
                        )
                        amount_ = amount.Amount(units, security)
                        new_entries.append(
                            data.Balance(fileloc, date, account_, amount_, None, None)
                        )

    new_entries.sort(key=lambda entry: entry.date)
    transactions = petl.fromdicts(rows).sort("dttrade")
    return new_entries, transactions, securities


def get_securities(soup):
    """Extract the list of securities from the OFX file."""

    seclistmsgsrsv = soup.find("seclistmsgsrsv1")
    if not seclistmsgsrsv:
        return

    securities = []
    for secinfo in seclistmsgsrsv.find_all("secinfo"):

        # Merge the two nodes in a dictionary.
        secid = souptodict(secinfo.find("secid"))
        secname = souptodict(secinfo.find("secname"))
        secid.update(secname)
        secid["name"] = secinfo.find("secname").contents[0]
        # Handle the Google collective trust accounts.
        if "ticker" not in secid:
            uniqueid = secid["uniqueid"]
            assert re.match(r"VGI00\d+$", uniqueid), secid
            secid["ticker"] = re.sub("VGI00", "VGI", uniqueid)
        securities.append(secid)

    return petl.fromdicts(securities)


_DESCRIPTION_MAP = petl.wrap(
    [
        ["tran", "tferaction", "description"],
        ["BUYMF", None, "Plan Contribution"],
        ["REINVEST", None, "Dividends on Equity Investments"],
        ["SELLMF  ", None, "Withdrawal"],
        # Note the repeat 2x.
        ["TRANSFER", "IN", "Fund to Fund In"],
        ["TRANSFER", "IN", "Plan Initiated TransferIn"],
        # Note the repeat 3x.
        ["TRANSFER", "OUT", "Fee"],
        ["TRANSFER", "OUT", "Fund to Fund Out"],
        ["TRANSFER", "OUT", "Plan Initiated TransferOut"],
    ]
)


def souptodict(node):
    """Convert all of the child nodes from BeautifulSoup node into a dict.
    This assumes the direct children are uniquely named, but this is often the
    case."""
    return {
        child.name: child.contents[0].strip()
        for child in node.contents
        if isinstance(child, bs4.element.Tag)
    }


def soup_get(node, name, conversion=None):
    "Find a child anywhere below node and return its value or None."
    child = node.find(name)
    if child:
        value = child.contents[0].strip()
        if conversion:
            value = conversion(value)
        return value


if __name__ == "__main__":
    importer = Importer(
        filing="Assets:US:Vanguard:Retire:PreTax",
        account_id=r"\d{6,8}",
        config={
            "asset_account": "Assets:US:Vanguard:{source}:{security}",
            "asset_balances": "Assets:US:Vanguard",
            "cash_account": "Assets:US:Vanguard:{source}:Cash",
            "income_account": "Income:US:Vanguard:{source}:{security}:{type}",
            "income_match": "Income:US:GoogleInc:{source}",
            "income_pnl": "Income:US:Vanguard:Retire:PnL",
            "expenses_fees": "Expenses:Financial:Fees:Vanguard",
            "source_pretax": "Retire:PreTax",
            "source_aftertax": "Retire:AfterTax",
            "source_match": "Retire:Match",
            "source_rollover": "Retire:Rollover",
            "source_othernonvest": "OtherNonVest",
        },
    )
    testing.main(importer)
