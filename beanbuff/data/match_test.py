from decimal import Decimal
import unittest

from beanbuff.data import match


class TestMatch(unittest.TestCase):

    def test_buy_sell(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+2), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'B'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'C'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'D'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'E'))
        self.assertEqual((Decimal(0), 'm-F'), inv.match(Decimal(-1), 'F'))

    def test_sell_buy(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(-2), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(-1), 'B'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'C'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'D'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'E'))
        self.assertEqual((Decimal(0), 'm-F'), inv.match(Decimal(+1), 'F'))

    def test_crossover(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-2), 'B'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+2), 'C'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'D'))
        self.assertEqual((Decimal(0), 'm-E'), inv.match(Decimal(-3), 'E'))

    def test_multiple(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'B'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'C'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'D'))
        self.assertEqual((Decimal(-4), 'm-A'), inv.match(Decimal(-5), 'E'))
        self.assertEqual((Decimal(1), 'm-A'), inv.match(Decimal(+1), 'F'))

    def test_expire_zero(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.expire('A'))
        self.assertEqual((Decimal(0), 'm-B'), inv.expire('B'))

    def test_expire_nonzero(self):
        inv = match.NanoInventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.expire('A'))

        self.assertEqual((Decimal(0), 'm-B'), inv.match(Decimal(-1), 'B'))
        self.assertEqual((Decimal(0), 'm-B'), inv.match(Decimal(-1), 'C'))
        self.assertEqual((Decimal(+2), 'm-B'), inv.expire('B'))


if __name__ == '__main__':
    unittest.main()
