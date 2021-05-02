from decimal import Decimal
import unittest

from beanbuff.tastyworks import tastyutils


ZERO = Decimal(0)


class DecimalTest(unittest.TestCase):

    def test_todecimal_na_values(self):
        self.assertEqual(ZERO, tastyutils.ToDecimal("--"))
        self.assertEqual(ZERO, tastyutils.ToDecimal("N/A"))
        self.assertEqual(ZERO, tastyutils.ToDecimal("N/A (Split Position)"))
        self.assertEqual(ZERO, tastyutils.ToDecimal(""))

    def test_todecimal_bond_32th(self):
        self.assertEqual(Decimal('100.'), tastyutils.ToDecimal("100'00"))
        self.assertEqual(Decimal('100.03125'), tastyutils.ToDecimal("100'01"))
        self.assertEqual(Decimal('100.5'), tastyutils.ToDecimal("100'16"))
        self.assertEqual(Decimal('100.96875'), tastyutils.ToDecimal("100'31"))
        self.assertEqual(Decimal('101.'), tastyutils.ToDecimal("100'32"))

        self.assertEqual(Decimal('100.'), tastyutils.ToDecimal("100'000"))
        self.assertEqual(Decimal('100.00390625'), tastyutils.ToDecimal("100'001"))
        self.assertEqual(Decimal('100.0078125'), tastyutils.ToDecimal("100'002"))

        with self.assertRaises(ValueError):
            tastyutils.ToDecimal("100'0001")

        with self.assertRaises(KeyError):
            tastyutils.ToDecimal("100'004")
        with self.assertRaises(KeyError):
            tastyutils.ToDecimal("100'009")

    def test_todecimal_bond_64th(self):
        self.assertEqual(Decimal('100.'), tastyutils.ToDecimal('100"00'))
        self.assertEqual(Decimal('100.015625'), tastyutils.ToDecimal('100"01'))
        self.assertEqual(Decimal('100.484375'), tastyutils.ToDecimal('100"31'))
        self.assertEqual(Decimal('100.5'), tastyutils.ToDecimal('100"32'))
        self.assertEqual(Decimal('100.984375'), tastyutils.ToDecimal('100"63'))
        self.assertEqual(Decimal('101.'), tastyutils.ToDecimal('100"64'))

        self.assertEqual(Decimal('100.'), tastyutils.ToDecimal('100"000'))
        self.assertEqual(Decimal('100.001953125'), tastyutils.ToDecimal('100"001'))
        self.assertEqual(Decimal('100.00390625'), tastyutils.ToDecimal('100"002'))

        with self.assertRaises(ValueError):
            tastyutils.ToDecimal('100"0001')

        with self.assertRaises(KeyError):
            tastyutils.ToDecimal('100"004')
        with self.assertRaises(KeyError):
            tastyutils.ToDecimal('100"009')


if __name__ == '__main__':
    unittest.main()
