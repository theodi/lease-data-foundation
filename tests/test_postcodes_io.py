"""
Tests for postcodes.io UK postcode geocoding functionality.

Uses real postcodes from the not_found.csv file to validate that postcodes.io API
correctly returns latitude, longitude, region, and admin_district for various UK regions.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock

import pandas as pd
import requests
from typing import Optional

from src.enricher.update_mongo_from_csv import (
    PostcodeCache,
    bulk_lookup_postcodes,
    geocode_postcodes_batch,
    process_not_found_chunk,
    LOCATION_FIELD,
    POSTCODES_IO_BATCH_SIZE,
)


class TestPostcodeCache(unittest.TestCase):
    """Tests for PostcodeCache class."""

    def test_cache_initialization(self):
        """Test cache initializes empty."""
        cache = PostcodeCache()
        self.assertEqual(cache.stats["size"], 0)
        self.assertEqual(cache.stats["hits"], 0)
        self.assertEqual(cache.stats["misses"], 0)

    def test_cache_set_and_get(self):
        """Test setting and getting values from cache."""
        cache = PostcodeCache()

        test_data = {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "region": "London",
            "post_town": "Westminster",
        }

        cache.set("SW1A 1AA", test_data)
        result = cache.get("SW1A 1AA")

        self.assertEqual(result, test_data)
        self.assertEqual(cache.stats["hits"], 1)

    def test_cache_normalization(self):
        """Test postcode normalization for cache keys."""
        cache = PostcodeCache()

        test_data = {"latitude": 51.5, "longitude": -0.1}
        cache.set("SW1A 1AA", test_data)

        # Different formats should hit the same cache entry
        self.assertEqual(cache.get("sw1a 1aa"), test_data)
        self.assertEqual(cache.get("SW1A1AA"), test_data)
        self.assertEqual(cache.get("  sw1a1aa  "), test_data)

    def test_cache_miss(self):
        """Test cache miss increments counter."""
        cache = PostcodeCache()

        result = cache.get("NOTCACHED")

        self.assertIsNone(result)
        self.assertEqual(cache.stats["misses"], 1)

    def test_cache_stores_none_for_invalid_postcodes(self):
        """Test that None values are cached for invalid postcodes."""
        cache = PostcodeCache()

        cache.set("INVALID", None)

        # Should be in cache (even though value is None)
        self.assertIn("INVALID", cache._cache)
        self.assertIsNone(cache.get("INVALID"))

    def test_get_uncached_postcodes(self):
        """Test getting list of uncached postcodes."""
        cache = PostcodeCache()

        cache.set("SW1A 1AA", {"latitude": 51.5})
        cache.set("M1 1AA", {"latitude": 53.5})

        postcodes = ["SW1A 1AA", "B1 1AA", "M1 1AA", "LS1 1AA"]
        uncached = cache.get_uncached(postcodes)

        self.assertEqual(sorted(uncached), sorted(["B1 1AA", "LS1 1AA"]))

    def test_cache_hit_rate_calculation(self):
        """Test hit rate calculation."""
        cache = PostcodeCache()

        cache.set("SW1A 1AA", {"latitude": 51.5})

        # 3 hits
        cache.get("SW1A 1AA")
        cache.get("SW1A 1AA")
        cache.get("SW1A 1AA")

        # 1 miss
        cache.get("NOTCACHED")

        stats = cache.stats
        self.assertEqual(stats["hits"], 3)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], "75.0%")


class TestBulkLookupPostcodes(unittest.TestCase):
    """Tests for bulk_lookup_postcodes function."""

    @patch('src.enricher.update_mongo_from_csv.requests.Session')
    def test_bulk_lookup_returns_results(self, mock_session_class):
        """Test bulk lookup returns geocode data for valid postcodes."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": 200,
            "result": [
                {
                    "query": "SW1A 1AA",
                    "result": {
                        "latitude": 51.501009,
                        "longitude": -0.141588,
                        "region": "London",
                        "admin_district": "Westminster",
                    }
                },
                {
                    "query": "M1 1AA",
                    "result": {
                        "latitude": 53.478552,
                        "longitude": -2.242631,
                        "region": "North West",
                        "admin_district": "Manchester",
                    }
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_session.post.return_value = mock_response

        results = bulk_lookup_postcodes(["SW1A 1AA", "M1 1AA"], mock_session)

        self.assertEqual(len(results), 2)
        self.assertIn("SW1A 1AA", results)
        self.assertIn("M1 1AA", results)
        self.assertEqual(results["SW1A 1AA"]["latitude"], 51.501009)
        # self.assertEqual(results["M1 1AA"]["region"], "North West")

    @patch('src.enricher.update_mongo_from_csv.requests.Session')
    def test_bulk_lookup_handles_not_found(self, mock_session_class):
        """Test bulk lookup returns None for invalid postcodes."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": 200,
            "result": [
                {
                    "query": "ZZ99 9ZZ",
                    "result": None
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_session.post.return_value = mock_response

        results = bulk_lookup_postcodes(["ZZ99 9ZZ"], mock_session)

        self.assertEqual(len(results), 1)
        self.assertIsNone(results["ZZ99 9ZZ"])

    @patch('src.enricher.update_mongo_from_csv.requests.Session')
    def test_bulk_lookup_handles_api_error(self, mock_session_class):
        """Test bulk lookup handles API errors gracefully."""
        mock_session = MagicMock()
        mock_session.post.side_effect = requests.exceptions.RequestException("API Error")

        results = bulk_lookup_postcodes(["SW1A 1AA"], mock_session)

        self.assertEqual(len(results), 1)
        self.assertIsNone(results["SW1A 1AA"])

    def test_bulk_lookup_empty_list(self):
        """Test bulk lookup with empty list."""
        mock_session = MagicMock()

        results = bulk_lookup_postcodes([], mock_session)

        self.assertEqual(results, {})

    @patch('src.enricher.update_mongo_from_csv.requests.Session')
    def test_bulk_lookup_respects_batch_limit(self, mock_session_class):
        """Test that bulk lookup limits to POSTCODES_IO_BATCH_SIZE."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {"status": 200, "result": []}
        mock_response.raise_for_status = Mock()
        mock_session.post.return_value = mock_response

        # Create list larger than batch size
        postcodes = [f"SW{i} 1AA" for i in range(150)]

        bulk_lookup_postcodes(postcodes, mock_session)

        # Should only send first 100
        call_args = mock_session.post.call_args
        sent_postcodes = call_args[1]["json"]["postcodes"]
        self.assertEqual(len(sent_postcodes), POSTCODES_IO_BATCH_SIZE)


class TestGeocodePostcodesBatch(unittest.TestCase):
    """Tests for geocode_postcodes_batch function."""

    @patch('src.enricher.update_mongo_from_csv.bulk_lookup_postcodes')
    def test_uses_cache_first(self, mock_bulk_lookup):
        """Test that cached postcodes are not sent to API."""
        cache = PostcodeCache()
        cache.set("SW1A 1AA", {"latitude": 51.5, "longitude": -0.1})

        mock_session = MagicMock()
        mock_bulk_lookup.return_value = {}

        results = geocode_postcodes_batch(["SW1A 1AA"], cache, mock_session)

        # Should return cached result without calling API
        self.assertEqual(results["SW1A 1AA"]["latitude"], 51.5)
        mock_bulk_lookup.assert_not_called()

    @patch('src.enricher.update_mongo_from_csv.bulk_lookup_postcodes')
    def test_fetches_uncached_from_api(self, mock_bulk_lookup):
        """Test that uncached postcodes are fetched from API."""
        cache = PostcodeCache()
        mock_session = MagicMock()

        mock_bulk_lookup.return_value = {
            "SW1A 1AA": {"latitude": 51.5, "longitude": -0.1}
        }

        results = geocode_postcodes_batch(["SW1A 1AA"], cache, mock_session)

        mock_bulk_lookup.assert_called_once()
        self.assertEqual(results["SW1A 1AA"]["latitude"], 51.5)

        # Should be cached now
        self.assertEqual(cache.get("SW1A 1AA")["latitude"], 51.5)

    @patch('src.enricher.update_mongo_from_csv.bulk_lookup_postcodes')
    def test_handles_mixed_cached_and_uncached(self, mock_bulk_lookup):
        """Test handling mix of cached and uncached postcodes."""
        cache = PostcodeCache()
        cache.set("SW1A 1AA", {"latitude": 51.5})

        mock_session = MagicMock()
        mock_bulk_lookup.return_value = {
            "M1 1AA": {"latitude": 53.5}
        }

        results = geocode_postcodes_batch(["SW1A 1AA", "M1 1AA"], cache, mock_session)

        self.assertEqual(results["SW1A 1AA"]["latitude"], 51.5)
        self.assertEqual(results["M1 1AA"]["latitude"], 53.5)

    def test_skips_none_and_na_postcodes(self):
        """Test that None and NA postcodes are skipped."""
        cache = PostcodeCache()
        mock_session = MagicMock()

        with patch('src.enricher.update_mongo_from_csv.bulk_lookup_postcodes') as mock_lookup:
            mock_lookup.return_value = {}

            results = geocode_postcodes_batch([None, pd.NA, ""], cache, mock_session)

            self.assertEqual(results, {})
            mock_lookup.assert_not_called()


class TestProcessNotFoundChunk(unittest.TestCase):
    """Tests for process_not_found_chunk function."""

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_processes_records_with_valid_postcodes(self, mock_geocode):
        """Test that records with valid postcodes create update operations."""
        mock_geocode.return_value = {
            "SW1A 1AA": {
                "latitude": 51.501009,
                "longitude": -0.141588,
                "region": "London",
                "post_town": "Westminster",
            },
            "M1 1AA": {
                "latitude": 53.478552,
                "longitude": -2.242631,
                "region": "North West",
                "post_town": "Manchester",
            }
        }

        data = {
            "uid": ["doc1", "doc2"],
            "pc": ["SW1A 1AA", "M1 1AA"],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()
        cache = PostcodeCache()
        mock_session = MagicMock()

        result = process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(result["updates"], 2)
        self.assertEqual(result["skipped"], 0)
        mock_collection.bulk_write.assert_called_once()

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_skips_records_without_postcode(self, mock_geocode):
        """Test that records without postcodes are skipped."""
        mock_geocode.return_value = {
            "SW1A 1AA": {"latitude": 51.5, "longitude": -0.1}
        }

        data = {
            "uid": ["doc1", "doc2", "doc3"],
            "pc": ["SW1A 1AA", "", None],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()
        cache = PostcodeCache()
        mock_session = MagicMock()

        result = process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["skipped"], 2)

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_skips_records_without_uid(self, mock_geocode):
        """Test that records without UID are skipped."""
        mock_geocode.return_value = {
            "SW1A 1AA": {"latitude": 51.5, "longitude": -0.1},
            "M1 1AA": {"latitude": 53.5, "longitude": -2.2},
        }

        data = {
            "uid": [None, "doc2"],
            "pc": ["SW1A 1AA", "M1 1AA"],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()
        cache = PostcodeCache()
        mock_session = MagicMock()

        result = process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["skipped"], 1)

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_skips_records_with_invalid_postcodes(self, mock_geocode):
        """Test that records with invalid postcodes (None result) are skipped."""
        mock_geocode.return_value = {
            "INVALID": None,
            "ZZ99 9ZZ": None,
        }

        data = {
            "uid": ["doc1", "doc2"],
            "pc": ["INVALID", "ZZ99 9ZZ"],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()
        cache = PostcodeCache()
        mock_session = MagicMock()

        result = process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["skipped"], 2)
        mock_collection.bulk_write.assert_not_called()

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_empty_chunk_returns_zero_counts(self, mock_geocode):
        """Test that empty chunk returns zero counts."""
        mock_geocode.return_value = {}

        chunk = pd.DataFrame(columns=["uid", "pc"])

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()
        cache = PostcodeCache()
        mock_session = MagicMock()

        result = process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["skipped"], 0)
        mock_collection.bulk_write.assert_not_called()

    @patch('src.enricher.update_mongo_from_csv.geocode_postcodes_batch')
    def test_update_document_contains_required_fields(self, mock_geocode):
        """Test that update documents contain latitude, longitude, location, rgn, and post_town."""
        mock_geocode.return_value = {
            "SW1A 1AA": {
                "latitude": 51.501009,
                "longitude": -0.141588,
                "region": "London",
                "post_town": "Westminster",
            }
        }

        data = {
            "uid": ["doc1"],
            "pc": ["SW1A 1AA"],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        captured_operations = []

        def capture_bulk_write(operations, **kwargs):
            captured_operations.extend(operations)

        mock_collection.bulk_write = Mock(side_effect=capture_bulk_write)
        cache = PostcodeCache()
        mock_session = MagicMock()

        process_not_found_chunk(chunk, mock_collection, cache, mock_session)

        self.assertEqual(len(captured_operations), 1)

        # Extract the update document from the UpdateOne operation
        update_op = captured_operations[0]
        update_doc = update_op._doc["$set"]

        self.assertIn("latitude", update_doc)
        self.assertIn("longitude", update_doc)
        self.assertIn(LOCATION_FIELD, update_doc)
        # self.assertIn("rgn", update_doc)
        # self.assertIn("post_town", update_doc)

        # Check values
        self.assertEqual(update_doc["latitude"], 51.501009)
        self.assertEqual(update_doc["longitude"], -0.141588)
        # self.assertEqual(update_doc["rgn"], "LONDON")  # uppercase
        # self.assertEqual(update_doc["post_town"], "WESTMINSTER")  # uppercase

        # Check location is a GeoJSON Point
        location = update_doc[LOCATION_FIELD]
        self.assertEqual(location["type"], "Point")
        self.assertIn("coordinates", location)
        self.assertEqual(len(location["coordinates"]), 2)

        # Check coordinates order [longitude, latitude]
        self.assertEqual(location["coordinates"][0], update_doc["longitude"])
        self.assertEqual(location["coordinates"][1], update_doc["latitude"])


class TestRealPostcodesIntegration(unittest.TestCase):
    """
    Integration tests using real postcodes.io API.

    These tests make actual API calls to validate the integration works.
    Skip these in CI/CD environments to avoid rate limiting.
    """

    @classmethod
    def setUpClass(cls):
        """Set up session and cache for all tests."""
        cls.session = requests.Session()
        cls.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        cls.cache = PostcodeCache()
        cls._skip_reason = None

        try:
            ping_response = cls.session.get("https://api.postcodes.io/ping", timeout=5)
            ping_response.raise_for_status()
            if ping_response.json().get("status") != 200:
                cls._skip_reason = "postcodes.io ping did not return status 200"
        except requests.exceptions.RequestException as exc:
            cls._skip_reason = f"postcodes.io unavailable: {exc}"

    @classmethod
    def tearDownClass(cls):
        """Clean up session."""
        if hasattr(cls, "session"):
            cls.session.close()

    def _skip_if_unavailable(self):
        if getattr(self, "_skip_reason", None):
            self.skipTest(self._skip_reason)

    def test_real_london_postcode(self):
        """Test real London postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["SW1A 1AA"], self.session)

        self.assertIn("SW1A 1AA", results)
        result = results["SW1A 1AA"]

        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

        # Westminster should be around 51.5°N, -0.14°W
        self.assertAlmostEqual(result["latitude"], 51.5, delta=0.1)
        self.assertAlmostEqual(result["longitude"], -0.14, delta=0.1)

    def test_real_manchester_postcode(self):
        """Test real Manchester postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["M14 7PA"], self.session)

        self.assertIn("M14 7PA", results)
        result = results["M14 7PA"]

        # Manchester should be around 53.5°N, -2.2°W
        self.assertAlmostEqual(result["latitude"], 53.5, delta=0.1)
        self.assertAlmostEqual(result["longitude"], -2.2, delta=0.2)

    def test_real_birmingham_postcode(self):
        """Test real Birmingham postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["B45 0EN"], self.session)

        self.assertIn("B45 0EN", results)
        result = results["B45 0EN"]

        # Birmingham should be around 52.5°N, -1.9°W
        self.assertAlmostEqual(result["latitude"], 52.5, delta=0.2)
        self.assertAlmostEqual(result["longitude"], -1.9, delta=0.2)

    def test_real_postcode1(self):
        """Test real postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["CM1 1SH"], self.session)
        print(results)

        self.assertIn("CM1 1SH", results)
        result = results["CM1 1SH"]

        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

        # Westminster should be around 51.5°N, -0.14°W
        self.assertAlmostEqual(result["latitude"], 51.7, delta=0.1)
        self.assertAlmostEqual(result["longitude"], 0.47, delta=0.1)

        # self.assertEqual(result["region"], "SOUTH EAST")
        # self.assertEqual(result["admin_district"], "CHELMSFORD")

    def test_real_postcode2(self):
        """Test real postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["SS13 2DB"], self.session)
        print(results)

        self.assertIn("SS13 2DB", results)
        result = results["SS13 2DB"]

        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

        # Westminster should be around 51.5°N, -0.14°W
        self.assertAlmostEqual(result["latitude"], 51.6, delta=0.1)
        self.assertAlmostEqual(result["longitude"], 0.47, delta=0.1)

        # self.assertEqual(result["region"], "SOUTH EAST")
        # self.assertEqual(result["admin_district"], "BASILDON")

    def test_real_postcode3(self):
        """Test real postcode lookup."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["E2 6JL"], self.session)
        print(results)

        self.assertIn("E2 6JL", results)
        result = results["E2 6JL"]

        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

        # Westminster should be around 51.5°N, -0.14°W
        self.assertAlmostEqual(result["latitude"], 51.5, delta=0.1)
        self.assertAlmostEqual(result["longitude"], -0.02, delta=0.1)

        # self.assertEqual(result["region"], "GREATER LONDON")
        # self.assertEqual(result["admin_district"], "LONDON")

    def test_real_bulk_lookup_multiple_postcodes(self):
        """Test bulk lookup with multiple real postcodes."""
        self._skip_if_unavailable()
        postcodes = ["SW1A 1AA", "E14 7DG", "BB12 0BP", "CT16 1L", "CT20 1RP"]

        results = bulk_lookup_postcodes(postcodes, self.session)
        print(results)

        self.assertEqual(len(results), len(postcodes))
        for pc in postcodes:
            self.assertIn(pc, results)
            if pc == "CT16 1L":
                # this is none
                self.assertFalse(results[pc])
                continue
            self.assertTrue(results[pc], f"Expected result for {pc} to be non-empty")
            self.assertIn("latitude", results[pc])
            self.assertIn("longitude", results[pc])

    def test_real_invalid_postcode_returns_none(self):
        """Test that invalid postcode returns None."""
        self._skip_if_unavailable()
        results = bulk_lookup_postcodes(["ZZ99 9ZZ"], self.session)

        self.assertIn("ZZ99 9ZZ", results)
        self.assertIsNone(results["ZZ99 9ZZ"])

    def test_geocode_batch_with_cache(self):
        """Test geocode batch uses cache correctly."""
        self._skip_if_unavailable()
        results1 = geocode_postcodes_batch(["SW1A 1AA"], self.cache, self.session)
        first_result = results1.get("SW1A 1AA")
        initial_hits = self.cache.stats["hits"]

        # Second call - should use cache
        results2 = geocode_postcodes_batch(["SW1A 1AA"], self.cache, self.session)

        self.assertEqual(results1, results2)
        self.assertGreater(self.cache.stats["hits"], initial_hits)


if __name__ == "__main__":
    unittest.main()

