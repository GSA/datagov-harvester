from unittest import TestCase
from unittest.mock import Mock, patch

from harvester.harvest import HarvestSource


class TestGeoCache(TestCase):
    def setUp(self):
        self.source = object.__new__(HarvestSource)
        self.source._geo_cache = {}
        self.source._db_interface = Mock()
        self.lookup = self.source._db_interface.query_geo_from_string
        self.lookup.return_value = '{"type": "Polygon"}'

    def test_repeat_lookup_is_served_from_cache(self):
        first = self.source.geo_from_string("Iowa")
        second = self.source.geo_from_string("Iowa")

        self.assertEqual(first, second)
        self.lookup.assert_called_once_with("Iowa", False)

    def test_literal_and_wildcard_lookups_are_cached_separately(self):
        self.source.geo_from_string("Fair_ax", literal=True)
        self.source.geo_from_string("Fair_ax")

        self.assertEqual(self.lookup.call_count, 2)

    def test_not_found_is_cached(self):
        self.lookup.return_value = None

        self.assertIsNone(self.source.geo_from_string("Nowhere"))
        self.assertIsNone(self.source.geo_from_string("Nowhere"))

        self.lookup.assert_called_once()

    def test_failed_lookup_is_not_cached(self):
        self.lookup.side_effect = [RuntimeError("db down"), '{"type": "Point"}']

        self.assertIsNone(self.source.geo_from_string("Ohio"))
        self.assertEqual(self.source.geo_from_string("Ohio"), '{"type": "Point"}')

    @patch("harvester.harvest.GEO_CACHE_SIZE", 1)
    def test_cache_stops_growing_at_its_limit(self):
        self.source.geo_from_string("Iowa")
        self.source.geo_from_string("Ohio")
        self.source.geo_from_string("Ohio")

        self.assertEqual(list(self.source._geo_cache), [("Iowa", False)])
        self.assertEqual(self.lookup.call_count, 3)
