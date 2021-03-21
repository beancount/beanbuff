#!/usr/bin/env python3
"""Import prices from one of the VGI downloadable files.

HistoricalPrices_Institutional_Extended_Market_Index_Trust.csv
"""

__copyright__ = "Copyright (C) 2020  Martin Blais"
__license__ = "GNU GPLv2"

import argparse
import datetime
import logging
import csv
import re
import sys
import subprocess
import os
import urllib.parse
from decimal import Decimal
from os import path
from typing import List, Tuple, Optional

import requests
import dateutil.parser
from dateutil import rrule
import bs4

#from dateutil import MONTHLY


_CACHE = False

def fetch_page_data(fund: str, year: int):
    """Fetch a single page of data."""

    # Fetch the page.
    params = {"results": "get",
              "FundIntExt": "INT",
              "FundId": fund,
              "beginDate": "01/01/{}".format(year),
              "endDate": "12/31/{}".format(year),
              "radio": "1",
              "radiobutton2": "1"}
    url = urllib.parse.urlunparse(
        ("https", "personal.vanguard.com", "/us/funds/tools/pricehistorysearch",
         "", urllib.parse.urlencode(params), ""))

    print(";; URL: {}".format(url))
    command = ["curl", "--referer", "https://investor.vanguard.com", url]
    if _CACHE:
        filename = "/tmp/cache.{}.{}".format(fund, year)
        if path.exists(filename):
            with open(filename) as ofile:
                output = ofile.read()
        else:
            output = subprocess.check_output(command, encoding="utf8",
                                             stderr=subprocess.DEVNULL)
            with open(filename, "w") as ofile:
                print(output, file=ofile)
    else:
        output = subprocess.check_output(command, encoding="utf8",
                                         stderr=subprocess.DEVNULL)

    # Find the data table.
    soup = bs4.BeautifulSoup(output, 'html.parser')
    for table in soup.findAll("table", class_="dataTable"):
        header = [th.text.strip() for th in table.findAll("th")]
        if header == ['Date', 'Price', 'Yield']:
            break
    else:
        return None

    # Parse each of the rows.
    dated_prices = []
    for tr in table.findAll("tr"):
        row = [td.text for td in tr.findAll("td")]
        if not row:
            continue
        date_str, price_str, _ = row
        date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
        price = Decimal(price_str.lstrip("$"))
        dated_prices.append((date, price))

    return dated_prices


def fetch_data(fund: str, requested_year: Optional[int]) -> List[Tuple[datetime.date, Decimal]]:
    """Fetch all data for a specific fund."""
    year_prices = []
    for index, year in enumerate(range(datetime.date.today().year, 0, -1)
                                 if not requested_year
                                 else [requested_year]):
        prices = fetch_page_data(fund, year)
        if prices is None:
            if index == 0:
                # If failed on the first page (Jan 1st or before 1st bizday
                # failure), continue.
                continue  
            else:
                break
        year_prices.append(prices)

    all_prices = []
    for prices in reversed(year_prices):
        all_prices.extend(prices)

    return all_prices


def get_ruleset(start_date: datetime.date):
    rset = rrule.rruleset()
    # Monthly on 1st or last valid date.
    rset.rrule(rrule.rrule(rrule.MONTHLY, bymonthday=(-1,),
                           dtstart=start_date,
                           until=datetime.date.today()))
    # Weekly every monday.
    rset.rrule(rrule.rrule(rrule.WEEKLY, byweekday=rrule.MO,
                           dtstart=start_date,
                           until=datetime.date.today()))
    return rset


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('funds', nargs='+',
                        help="Four-digit fund numbers.")
    parser.add_argument('-o', '--output-dir', action='store',
                        help="Output directory name.")
    parser.add_argument('-r', '--restricted-dates', action='store_true',
                        help="Output only last day of month + mondays.")
    parser.add_argument('-y', '--year', action='store', type=int,
                        help="Fetch for a single, given year")

    args = parser.parse_args()

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)


    for fund in args.funds:
        assert re.match(r"\d{4}", fund), "Invalid fund number."
        prices = fetch_data(fund, args.year)
        if args.restricted_dates:
            rset = get_ruleset(prices[0][0])
            dates = [dt.date() for dt in rset]

        if args.output_dir:
            # Save output to individual CSV files for each fund.
            filename = path.join(args.output_dir, "{}.csv".format(fund))
            with open(filename, "w") as ofile:
                writer = csv.writer(ofile)
                for date, price in prices:
                    if args.restricted_dates and date not in dates:
                        continue
                    writer.writerow([date.isoformat(), str(price)])
        else:
            # Print output to price directives.
            print()
            instrument = "VGI00{:4}".format(fund)
            for date, price in prices:
                if args.restricted_dates and date not in dates:
                    continue
                print("{:%Y-%m-%d} price {} {} {}".format(date, instrument, price, "USD"))
            print()


if __name__ == '__main__':
    main()
