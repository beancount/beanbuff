"""Price source for the Vanguard Google collective trusts.

This depends on a service created by a Googler on Cloud.

https://us-central1-bramp-projects.cloudfunctions.net/vanguard/7553

<fund>
  <id>1898</id>
  <citFundId>7553</citFundId>
  <ticker/>
  <name>Vanguard Institutional Extended Market Index Trust</name>
  <shortName>Instl Ext Market Idx Tr</shortName>
  <expenseRatio>0.0200</expenseRatio>
  <price>99.19</price>
  <priceAsOfDate>2018-03-02T00:00:00-05:00</priceAsOfDate>
</fund>

"""

import re
import datetime

from beancount.core.number import D
from beanprice import net_utils

from beanprice import source

from dateutil import tz
import bs4
from dateutil import tz


def parse_datetime(string):
    return datetime.datetime.strptime(string[:10], '%Y-%m-%d')


class Source(source.Source):
    "Price extractor for Google Vanguard collective trusts."

    URL = "https://us-central1-bramp-projects.cloudfunctions.net/vanguard/{number}"
    def get_latest_price(self, ticker):
        match = re.match(r"VGI00(\d{4})", ticker)
        assert match, "Invalid ticker for importer {}: '{}'".format(self, ticker)

        url = self.URL.format(number=match.group(1))
        response = net_utils.retrying_urlopen(url)

        soup = bs4.BeautifulSoup(response.read().decode("utf-8"), "lxml")
        fund = soup.find("fund")
        price = D(fund.find("price").text)
        time = parse_datetime(fund.find("priceasofdate").text)
        us_timezone = tz.gettz("America/New_York")
        time = time.astimezone(us_timezone)

        return source.SourcePrice(price, time, "USD")

