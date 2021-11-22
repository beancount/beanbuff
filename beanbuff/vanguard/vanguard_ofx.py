"""OFX file format importer for investment accounts.
"""
import datetime
import itertools
import re
from typing import Dict, Optional
from os import path

import bs4

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
from beangulp.importers.mixins import config
from beangulp.importers.mixins import filing
from beangulp.importers.mixins import identifier
import beangulp


CONFIG = {
    'asset_account'       : ('Asset accounts '
                             '(include {source} for subaccount, {security} for security)'),
    'asset_balances'      : ('Root account for balances of a particular security '
                             '(include {source} for subaccount, {security} for security)'),
    'cash_account'        : ('Cash accounts '
                             '(include {source} for subaccount, {security} for security)'),
    'income_account'      : ('Income accounts '
                             '(include {source} for subaccount, {security} for security, '
                             '{type} for income type)'),
    'income_match'        : ('Source account for match contributions '
                             '(include {source} for subaccount, {security} for security)'),
    'income_pnl'          : 'P/L income account',
    'expenses_fees'       : 'Fees expenses',
    'source_pretax'       : 'Name of pre-tax subaccount',
    'source_aftertax'     : 'Name of after-tax subaccount',
    'source_match'        : 'Name of match contributions subaccount',
    'source_rollover'     : 'Name of rollover subaccount',
    'source_othernonvest' : 'Name of other/non-vesting subaccount',
}


class Importer(beangulp.Importer):

    def __init__(self, filing: str, account_id: str, config: Dict[str, str]):
        self._account = filing
        self._account_id = account_id
        self.config = config
        utils.validate_accounts(CONFIG, config)

    def identify(self, filepath: str) -> bool:
        return (utils.is_mimetype(filepath, {'application/x-ofx',
                                             'application/vnd.intu.qfx'}) and
                utils.search_file_regexp(filepath, r"Vanguard", encoding='cp1252') and
                utils.search_file_regexp(
                    filepath, r"<ACCTID>{}".format(self._account_id), encoding='cp1252'))

    def account(self, filepath: str) -> data.Account:
        return self._account

    def date(self, filepath: str) -> Optional[datetime.date]:
        return None

    def filename(self, filepath: str) -> Optional[str]:
        return 'vanguard.{}'.format(path.basename(filepath))

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        return extract(filepath, self._account_id, self.config, flags.FLAG_OKAY)


def extract(filename: str, account_id: str, config: Dict[str, str], flag: str) -> data.Entries:
    """Extract transaction info from the given OFX file into transactions for the
    given account. This function returns a list of entries possibly partially
    filled entries.
    """

    # Prepare mappings to accounts from the config provided.
    sources = {
        'PRETAX'       : config['source_pretax'],
        'MATCH'        : config['source_match'],
        'AFTERTAX'     : config['source_aftertax'],
        'ROLLOVER'     : config['source_rollover'],
        'OTHERNONVEST' : config['source_othernonvest'],
    }

    def get_other_account(trantype, source, _incometype):
        if trantype == 'BUYMF' and source == 'MATCH':
            return config['income_match'].format(source='{source}',
                                                 security='{security}')
        elif trantype == 'REINVEST':
            return config['income_account'].format(source='{source}',
                                                   security='{security}',
                                                   type='Dividend')
        elif trantype == 'TRANSFER':
            return config['expenses_fees']
        else:
            return config['cash_account'].format(source='{source}',
                                                 security='{security}')

    new_entries = []

    # Parse the XML file.
    with open(filename, errors='ignore') as infile:
        soup = bs4.BeautifulSoup(infile, 'lxml')

    # Get the description of securities used in this file.
    securities = get_securities(soup)
    if securities:
        securities_map = {security['uniqueid']: security
                          for security in securities}

    # For each statement.
    txn_counter = itertools.count()
    for stmtrs in soup.find_all(re.compile('.*stmtrs$')):
        # account_type = stmtrs.find('accttype').text.strip()
        # bank_id = stmtrs.find('bankid').text.strip()
        acctid = stmtrs.find('acctid').text.strip()
        if acctid != account_id:
            continue

        # For each currency.
        for currency_node in stmtrs.find_all('curdef'):
            currency = currency_node.contents[0].strip()

            # Process all investment transaction lists.
            # Note: this was developed for Vanguard.
            for invtranlist in stmtrs.find_all(re.compile('(invtranlist)')):

                for tran in invtranlist.find_all(re.compile('(buymf|sellmf|reinvest|buystock|sellstock|buyopt|sellopt|transfer)')):
                    date = parse_ofx_time(soup_get(tran, 'dttrade')).date()

                    uniqueid = soup_get(tran, 'uniqueid')
                    security = securities_map[uniqueid]['ticker']

                    units = soup_get(tran, 'units', D)
                    unitprice = soup_get(tran, 'unitprice', D)
                    total = soup_get(tran, 'total', D)

                    fileloc = data.new_metadata(filename, next(txn_counter))
                    payee = None

                    trantype = tran.name.upper()
                    incometype = soup_get(tran, 'incometype')
                    source = soup_get(tran, 'inv401ksource')
                    memo = soup_get(tran, 'memo')
                    narration = ' - '.join(filter(None, (trantype, incometype, source, memo)))

                    if source is None:
                        msg = "Could not establish source for {} on {}: '''\n{}'''".format(security, date, tran.prettify())
                        if tran.name in ('sellmf', 'reinvest'):
                            # When we sell anything without a source, we
                            # assume it's always for an after-tax to roth
                            # conversion.
                            source = 'AFTERTAX'
                        else:
                            raise ValueError(msg)

                    entry = data.Transaction(fileloc, date, flag, payee, narration, EMPTY_SET, EMPTY_SET, [])

                    # Create stock posting.
                    tferaction = soup_get(tran, 'tferaction')
                    if tferaction == 'OUT':
                        assert units < ZERO


                    units_amount = amount.Amount(units, security)
                    cost = position.Cost(unitprice, currency, None, None)
                    account_sec = config['asset_account'].format(source=sources[source],
                                                                 security=security)
                    entry.postings.append(data.Posting(account_sec, units_amount, cost, None, None, None))

                    # Compute total amount.
                    if tran.name == 'transfer':
                        assert total is None
                        total = -(units * unitprice)
                    elif tran.name == 'buymf':
                        assert total is not None
                        assert abs(total + (units * unitprice)) < 0.005, abs(total - (units * unitprice))
                    elif tran.name == 'reinvest':
                        assert total is not None
                        assert abs(abs(total) - abs(units * unitprice)) < 0.005, abs(abs(total) - abs(units * unitprice))
                        if not re.search('DIVIDEND REINVESTMENT', memo):
                            # This is going to get booked to a dividend leg; invert the sign.
                            total = -total

                    # Create cash posting.
                    account_template = get_other_account(trantype, source, incometype)
                    account_cash = account_template.format(source=sources[source],
                                                           security=security)
                    entry.postings.append(data.Posting(account_cash, amount.Amount(total, currency), None, None, None, None))

                    # On a sale, add a leg to absorb the P/L.
                    if tran.name == 'sellmf':
                        account_pnl = config['income_pnl']
                        entry.postings.append(data.Posting(account_pnl, None, None, None, None, None))

                    new_entries.append(entry)

                # Process the cash account transactions.
                for tran in invtranlist.find_all(re.compile('(invbanktran)')):
                    date = parse_ofx_time(soup_get(tran, 'dtposted')).date()
                    number = soup_get(tran, 'trnamt', D)
                    name = soup_get(tran, 'name')
                    memo = soup_get(tran, 'memo')
                    fitid = soup_get(tran, 'fitid')
                    subacctfund = soup_get(tran, 'subacctfund')

                    assert subacctfund == 'CASH' # I don't know what the other transaction types could be.

                    fileloc = data.new_metadata(filename, next(txn_counter))

                    narration = '{} - {}'.format(name, memo)
                    account_ = get_other_account('OTHER', 'AFTERTAX', None).format(source=sources[source])
                    entry = data.Transaction(fileloc, date, flag, None, narration, EMPTY_SET, {fitid}, [
                        data.Posting(account_, amount.Amount(number, currency), None, None, None, None)
                    ])

                    new_entries.append(entry)

                # Process all positions, convert them to data.Balance directives.
                # Note: this was developed for Vanguard.
                for invposlist in stmtrs.find_all('invposlist'):
                    for invpos in invposlist.find_all('invpos'):
                        date = parse_ofx_time(soup_get(invpos, 'dtpriceasof')).date() + datetime.timedelta(days=1)

                        uniqueid = soup_get(invpos, 'uniqueid')
                        security = securities_map[uniqueid]['ticker']

                        units = soup_get(invpos, 'units', D)
                        unitprice = soup_get(invpos, 'unitprice', D)

                        fileloc = data.new_metadata(filename, next(txn_counter))
                        source = soup_get(invpos, 'inv401ksource')
                        if source is None:
                            continue
                        account_ = config['asset_balances'].format(source=sources[source],
                                                                   security=security)
                        amount_ = amount.Amount(units, security)
                        new_entries.append(data.Balance(fileloc, date, account_, amount_,
                                                        None, None))

    new_entries.sort(key=lambda entry: entry.date)
    return new_entries


def get_securities(soup):
    """Extract the list of securities from the OFX file."""

    seclistmsgsrsv = soup.find('seclistmsgsrsv1')
    if not seclistmsgsrsv:
        return

    securities = []
    for secinfo in seclistmsgsrsv.find_all('secinfo'):

        # Merge the two nodes in a dictionary.
        secid = souptodict(secinfo.find('secid'))
        secname = souptodict(secinfo.find('secname'))
        secid.update(secname)
        secid['name'] = secinfo.find('secname').contents[0]
        # Handle the Google collective trust accounts.
        if 'ticker' not in secid:
            ticker = secid['ticker'] = secid['uniqueid']
            assert re.match(r'VGI00\d+$', ticker), ticker
        securities.append(secid)

    return securities


def souptodict(node):
    """Convert all of the child nodes from BeautifulSoup node into a dict.
    This assumes the direct children are uniquely named, but this is often the
    case."""
    return {child.name: child.contents[0].strip()
            for child in node.contents
            if isinstance(child, bs4.element.Tag)}


def soup_get(node, name, conversion=None):
    "Find a child anywhere below node and return its value or None."
    child = node.find(name)
    if child:
        value = child.contents[0].strip()
        if conversion:
            value = conversion(value)
        return value


if __name__ == '__main__':
    importer = Importer(filing='Assets:US:Vanguard:Retire:PreTax',
                        account_id='87654321',
                        config={
        'asset_account'       : 'Assets:US:Vanguard:{source}:{security}',
        'asset_balances'      : 'Assets:US:Vanguard',
        'cash_account'        : 'Assets:US:Vanguard:{source}:Cash',
        'income_account'      : 'Income:US:Vanguard:{source}:{type}',
        'income_match'        : 'Income:US:GoogleInc:{source}',
        'income_pnl'          : 'Income:US:Vanguard:Retire:PnL',
        'expenses_fees'       : 'Expenses:Financial:Fees:Vanguard',
        'source_pretax'       : 'Retire:PreTax',
        'source_aftertax'     : 'Retire:AfterTax',
        'source_match'        : 'Retire:Match',
        'source_rollover'     : 'Retire:Rollover',
        'source_othernonvest' : 'OtherNonVest',
    })
    testing.main(importer)
