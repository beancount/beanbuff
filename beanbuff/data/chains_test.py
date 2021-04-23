import itertools
import datetime
import functools
import unittest

from beanbuff.data.chains import Txn, TxnInventory


def genids(prefix):
    for id_ in itertools.count():
        yield "{}{:02d}".format(prefix, id_)


def create_txn(instruction, effect, quantity, idgen):
    return Txn(datetime.date(2021, 3, 1),
               next(idgen), None, None,
               'TRADE', 'ABC', instruction, effect, quantity,
               0, 0, 0, False, None)


def get_match_ids(matched):
    return [set(t.transactionId for t in match)
            for match in matched]


class TestTxnGroups(unittest.TestCase):

    def test_long(self):
        inv = TxnInventory()
        mids = genids('m')

        create = functools.partial(create_txn, idgen=genids('t'))

        matched = inv.match(create('BUY', 'OPENING', 1), mids)
        self.assertEqual([], get_match_ids(matched))
        self.assertEqual(set(['t00']), set(t.transactionId for t in inv.txns))

        matched = inv.match(create('BUY', 'OPENING', 2), mids)
        self.assertEqual([], get_match_ids(matched))
        self.assertEqual(set(['t00', 't01']), set(t.transactionId for t in inv.txns))

        matched = inv.match(create('SELL', 'OPENING', -2), mids)
        self.assertEqual([{'t02', 't00'}, {'t02', 't01'}], get_match_ids(matched))
        self.assertEqual({'t01'}, set(t.transactionId for t in inv.txns))

    def test_short(self):
        inv = TxnInventory()
        mids = genids('m')
        create = functools.partial(create_txn, idgen=genids('t'))

        matched = inv.match(create('BUY', 'OPENING', -1), mids)
        self.assertEqual([], get_match_ids(matched))
        self.assertEqual(set(['t00']), set(t.transactionId for t in inv.txns))

        matched = inv.match(create('BUY', 'OPENING', -2), mids)
        self.assertEqual([], get_match_ids(matched))
        self.assertEqual(set(['t00', 't01']), set(t.transactionId for t in inv.txns))

        matched = inv.match(create('SELL', 'OPENING', 2), mids)
        self.assertEqual([{'t02', 't00'}, {'t02', 't01'}], get_match_ids(matched))
        self.assertEqual({'t01'}, set(t.transactionId for t in inv.txns))


if __name__ == '__main__':
    unittest.main()
