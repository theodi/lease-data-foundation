"""
Tests for the update_mongo_from_csv enricher script.
"""

import unittest
import pandas as pd
from unittest.mock import Mock

from src.enricher.update_mongo_from_csv import (
    is_residential,
    process_chunk,
    ADDRESS_FIELD_MAPPING,
    RESIDENTIAL_CLASSES,
)


class TestIsResidential(unittest.TestCase):
    """Tests for is_residential function."""

    def test_residential_r_class(self):
        """Test R class is residential."""
        self.assertTrue(is_residential("R"))
        self.assertTrue(is_residential("R     "))
        self.assertTrue(is_residential("RD123"))

    def test_residential_x_class(self):
        """Test X class is residential."""
        self.assertTrue(is_residential("X"))
        self.assertTrue(is_residential("X     "))

    def test_residential_p_class(self):
        """Test P class is residential."""
        self.assertTrue(is_residential("P"))
        self.assertTrue(is_residential("P     "))

    def test_non_residential_c_class(self):
        """Test C class (commercial) is not residential."""
        self.assertFalse(is_residential("C"))
        self.assertFalse(is_residential("C     "))
        self.assertFalse(is_residential("CR01"))

    def test_non_residential_other_classes(self):
        """Test other classes are not residential."""
        self.assertFalse(is_residential("L"))  # Land
        self.assertFalse(is_residential("O"))  # Other
        self.assertFalse(is_residential("Z"))  # Object of interest

    def test_empty_and_null_values(self):
        """Test handling of empty and null values."""
        self.assertFalse(is_residential(""))
        self.assertFalse(is_residential(None))
        self.assertFalse(is_residential(pd.NA))


class TestProcessChunk(unittest.TestCase):
    """Tests for process_chunk function."""

    def test_residential_records_create_updates(self):
        """Test that residential records generate UpdateOne operations."""
        data = {
            "uid": ["doc1", "doc2"],
            "class": ["R     ", "P     "],
            "uprn": [123456, 789012],
            "udprn": [111, 222],
            "building_name": ["Building A", "Building B"],
            "building_number": ["1", "2"],
            "thoroughfare": ["Main Street", "High Street"],
            "post_town": ["London", "Manchester"],
            "postcode": ["SW1A 1AA", "M1 1AA"],
            "x_coordinate": [100.0, 200.0],
            "y_coordinate": [100.0, 200.0],
            "latitude": [51.5, 53.5],
            "longitude": [-0.1, -2.2],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()

        result = process_chunk(chunk, mock_collection)

        self.assertEqual(result["updates"], 2)
        self.assertEqual(result["deletes"], 0)
        mock_collection.bulk_write.assert_called_once()

    def test_non_residential_records_create_deletes(self):
        """Test that non-residential records generate DeleteOne operations."""
        data = {
            "uid": ["doc1", "doc2"],
            "class": ["C     ", "L     "],
            "uprn": [123456, 789012],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()

        result = process_chunk(chunk, mock_collection)

        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["deletes"], 2)
        mock_collection.bulk_write.assert_called_once()

    def test_mixed_records(self):
        """Test processing of mixed residential and non-residential records."""
        data = {
            "uid": ["doc1", "doc2", "doc3"],
            "class": ["R     ", "C     ", "P     "],
            "uprn": [123456, 789012, 345678],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()

        result = process_chunk(chunk, mock_collection)

        self.assertEqual(result["updates"], 2)
        self.assertEqual(result["deletes"], 1)

    def test_empty_chunk(self):
        """Test processing of empty chunk."""
        chunk = pd.DataFrame(columns=["uid", "class", "uprn"])

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()

        result = process_chunk(chunk, mock_collection)

        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["deletes"], 0)
        mock_collection.bulk_write.assert_not_called()

    def test_missing_uid_skipped(self):
        """Test that records without UID are skipped."""
        data = {
            "uid": [None, "doc2"],
            "class": ["R     ", "R     "],
            "uprn": [123456, 789012],
        }
        chunk = pd.DataFrame(data)

        mock_collection = Mock()
        mock_collection.bulk_write = Mock()

        result = process_chunk(chunk, mock_collection)

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["deletes"], 0)


class TestAddressFieldMapping(unittest.TestCase):
    """Tests for address field mapping."""

    def test_mapping_contains_required_fields(self):
        """Test that all required fields are in the mapping."""
        required_csv_fields = [
            "uprn", "udprn", "building_name", "building_number",
            "thoroughfare", "post_town", "postcode",
            "x_coordinate", "y_coordinate", "latitude", "longitude",
        ]
        for field in required_csv_fields:
            self.assertIn(field, ADDRESS_FIELD_MAPPING)

    def test_uprn_mapped_to_ab_uprn(self):
        """Test that uprn is mapped to ab_uprn."""
        self.assertEqual(ADDRESS_FIELD_MAPPING["uprn"], "ab_uprn")

    def test_postcode_mapped_to_ab_postcode(self):
        """Test that postcode is mapped to ab_postcode."""
        self.assertEqual(ADDRESS_FIELD_MAPPING["postcode"], "ab_postcode")


class TestResidentialClasses(unittest.TestCase):
    """Tests for residential classes constant."""

    def test_contains_r_x_p(self):
        """Test that R, X, P are in residential classes."""
        self.assertIn("R", RESIDENTIAL_CLASSES)
        self.assertIn("X", RESIDENTIAL_CLASSES)
        self.assertIn("P", RESIDENTIAL_CLASSES)

    def test_does_not_contain_commercial(self):
        """Test that commercial classes are not residential."""
        self.assertNotIn("C", RESIDENTIAL_CLASSES)
        self.assertNotIn("L", RESIDENTIAL_CLASSES)
        self.assertNotIn("O", RESIDENTIAL_CLASSES)


if __name__ == "__main__":
    unittest.main()

