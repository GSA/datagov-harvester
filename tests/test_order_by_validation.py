import unittest

from database.interface import InvalidOrderByException, order_by_helper
from database.models import HarvestSource


class TestOrderByValidation(unittest.TestCase):
    def test_invalid_order_by_field(self):
        with self.assertRaisesRegex(
            InvalidOrderByException,
            "Invalid order_by field: notafield",
        ):
            order_by_helper(HarvestSource, "notafield")


if __name__ == "__main__":
    unittest.main()
