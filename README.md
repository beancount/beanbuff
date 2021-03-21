# beanbuff: Beancount Importers for Brokerage Accounts

This repository contains public importers for various brokerage accounts. They
are consolidated here in order leverage common utility functions and libraries
between them, and to allow for analysis code that works on and across all of
them, integrated with Beancount.

The codes in this repository should support equity, equity options, futures, and
futures options. Translations will be available to and from Beancount in order
allow centralized analysis of performance in aggregate, across multiple brokers.


## Status

This is work in constant progress. It's rough around the edges and is
specifically designed for my personal reconciliation workflow. Use it at your
own peril or benefit.

NOTE: Many of these importers are currently being rewritten and reconsolidated
in order to perform centralized analysis. Therefore, some of the stuff will not
be in working order as of March 2021.


## Testing

These codes are tested on real data that may or may not include some of the
transactions you're seeing in your account (the test data is not shared for
privacy reasons). If you have transactions that do not parse by one of these
importers, please contact us and if possible, share an anonymized file so
support can be expanded to cover the functionality.

See the [Beangulp](http://github.com/beancount/beangulp) project for general
running and testing information.


## Dependencies

This code is built on top of [Beancount
v3](http://github.com/beancount/beancount) and
[Beangulp](http://github.com/beancount/beangulp).


## Mailing-list

Questions related to this code should be sent to the [Beancount
mailing-list](https://groups.google.com/g/beancount).


## History

These codes used to live in various repositories and were consolidated here in
March 2021.
