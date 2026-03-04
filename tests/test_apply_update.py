"""
Tests for the apply_update script.
"""

import unittest

from src.data.apply_update import (
    extract_postcode,
    map_row,
    normalize_value,
    calculate_char_differences,
    FIELD_MAP,
)


class TestExtractPostcode(unittest.TestCase):
    """Test postcode extraction from property descriptions."""

    def test_extract_valid_postcode(self):
        """Test extraction of a valid postcode."""
        row = {
            "Register Property Description": "123 Main Street, London, SW1A 1AA",
            "Associated Property Description": "",
        }
        result = extract_postcode(row)
        self.assertEqual(result, "SW1A 1AA")

    def test_extract_postcode_no_space(self):
        """Test extraction and normalization of postcode without space."""
        row = {
            "Register Property Description": "Property in SW1A1AA area",
            "Associated Property Description": "",
        }
        result = extract_postcode(row)
        self.assertEqual(result, "SW1A1AA")

    def test_extract_from_associated_description(self):
        """Test extraction from associated property description."""
        row = {
            "Register Property Description": "123 Main Street",
            "Associated Property Description": "London, E1 6AN",
        }
        result = extract_postcode(row)
        self.assertEqual(result, "E1 6AN")

    def test_no_postcode_found(self):
        """Test when no postcode is present."""
        row = {
            "Register Property Description": "Some property",
            "Associated Property Description": "No postcode here",
        }
        result = extract_postcode(row)
        self.assertIsNone(result)

    def test_lowercase_postcode_normalized(self):
        """Test that lowercase postcodes are normalized to uppercase."""
        row = {
            "Register Property Description": "Property at sw1a 1aa",
            "Associated Property Description": "",
        }
        result = extract_postcode(row)
        self.assertEqual(result, "SW1A 1AA")


class TestMapRow(unittest.TestCase):
    """Test row mapping from CSV format to MongoDB format."""

    def test_map_all_fields(self):
        """Test mapping of all fields."""
        original_row = {
            "Unique Identifier": "123456789",
            "Register Property Description": "123 Main St, London SW1A 1AA",
            "County": "DEVON",
            "Region": "SOUTH WEST",
            "Associated Property Description ID": "APID123",
            "Associated Property Description": "Ground floor flat",
            "OS UPRN": "100023456789",
            "Price Paid": "250000",
            "Reg Order": "1",
            "Date of Lease": "01/01/2020",
            "Term": "99 years",
            "Alienation Clause Indicator": "Y",
        }

        result = map_row(original_row)

        self.assertEqual(result["uid"], "123456789")
        self.assertEqual(result["rpd"], "123 Main St, London SW1A 1AA")
        self.assertEqual(result["cty"], "DEVON")
        self.assertEqual(result["rgn"], "SOUTH WEST")
        self.assertEqual(result["apid"], "APID123")
        self.assertEqual(result["apd"], "Ground floor flat")
        self.assertEqual(result["uprn"], "100023456789")
        self.assertEqual(result["ppd"], "250000")
        self.assertEqual(result["ro"], "1")
        self.assertEqual(result["dol"], "01/01/2020")
        self.assertEqual(result["term"], "99 years")
        self.assertEqual(result["aci"], "Y")
        self.assertEqual(result["pc"], "SW1A 1AA")

    def test_map_missing_fields(self):
        """Test mapping with missing fields."""
        original_row = {
            "Unique Identifier": "123456789",
            "Register Property Description": "Property",
        }

        result = map_row(original_row)

        self.assertEqual(result["uid"], "123456789")
        self.assertEqual(result["rpd"], "Property")
        self.assertEqual(result["cty"], "")
        self.assertEqual(result["apid"], "")

    def test_map_strips_whitespace(self):
        """Test that mapping strips leading/trailing whitespace."""
        original_row = {
            "Unique Identifier": "  123456789  ",
            "County": "  DEVON  ",
        }

        result = map_row(original_row)

        self.assertEqual(result["uid"], "123456789")
        self.assertEqual(result["cty"], "DEVON")


class TestNormalizeValue(unittest.TestCase):
    """Test value normalization."""

    def test_normalize_string(self):
        """Test normalizing a string."""
        self.assertEqual(normalize_value("  test  "), "test")

    def test_normalize_none(self):
        """Test normalizing None."""
        self.assertEqual(normalize_value(None), "")

    def test_normalize_number(self):
        """Test normalizing a number."""
        self.assertEqual(normalize_value(123), "123")

    def test_normalize_empty_string(self):
        """Test normalizing empty string."""
        self.assertEqual(normalize_value(""), "")


class TestCalculateCharDifferences(unittest.TestCase):
    """Test character difference calculation."""

    def test_identical_records(self):
        """Test when records are identical."""
        original_row = {
            "Unique Identifier": "123",
            "County": "DEVON",
        }
        db_record = {
            "uid": "123",
            "cty": "DEVON",
        }

        total_diffs, details = calculate_char_differences(original_row, db_record)

        self.assertEqual(total_diffs, 0)
        self.assertEqual(details, [])

    def test_single_field_difference(self):
        """Test single field with differences."""
        original_row = {
            "Unique Identifier": "123",
            "County": "DEVON",
            "Region": "SOUTH WEST",
        }
        db_record = {
            "uid": "123",
            "cty": "Devon",  # Different case
            "rgn": "SOUTH WEST",
        }

        total_diffs, details = calculate_char_differences(original_row, db_record)

        self.assertGreater(total_diffs, 0)
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["field"], "County")
        self.assertEqual(details[0]["csv_val"], "DEVON")
        self.assertEqual(details[0]["db_val"], "Devon")

    def test_multiple_field_differences(self):
        """Test multiple fields with differences."""
        original_row = {
            "Unique Identifier": "123",
            "County": "DEVON",
            "Region": "SOUTH",
        }
        db_record = {
            "uid": "124",  # Different UID
            "cty": "Devon",  # Different case
            "rgn": "SOUTH WEST",  # Different content
        }

        total_diffs, details = calculate_char_differences(original_row, db_record)

        self.assertGreater(total_diffs, 0)
        self.assertEqual(len(details), 3)  # All three fields differ

    def test_one_char_difference(self):
        """Test single character difference."""
        original_row = {
            "Unique Identifier": "123",
            "County": "DEVON",
        }
        db_record = {
            "uid": "123",
            "cty": "DEVAN",  # O -> A (1 char diff)
        }

        total_diffs, details = calculate_char_differences(original_row, db_record)

        self.assertEqual(total_diffs, 1)
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["char_diff"], 1)

    def test_length_difference(self):
        """Test when strings have different lengths."""
        original_row = {
            "Unique Identifier": "123",
            "County": "DEVON",
        }
        db_record = {
            "uid": "123",
            "cty": "DEV",  # Shorter
        }

        total_diffs, details = calculate_char_differences(original_row, db_record)

        self.assertEqual(total_diffs, 2)  # "ON" missing = 2 chars
        self.assertEqual(len(details), 1)


class TestFieldMap(unittest.TestCase):
    """Test the field mapping constant."""

    def test_field_map_keys(self):
        """Test that FIELD_MAP has expected keys."""
        expected_keys = {
            "Unique Identifier",
            "Register Property Description",
            "County",
            "Region",
            "Associated Property Description ID",
            "Associated Property Description",
            "OS UPRN",
            "Price Paid",
            "Reg Order",
            "Date of Lease",
            "Term",
            "Alienation Clause Indicator",
        }
        self.assertEqual(set(FIELD_MAP.keys()), expected_keys)

    def test_field_map_values(self):
        """Test that FIELD_MAP has expected short keys."""
        expected_values = {
            "uid", "rpd", "cty", "rgn", "apid", "apd",
            "uprn", "ppd", "ro", "dol", "term", "aci",
        }
        self.assertEqual(set(FIELD_MAP.values()), expected_values)


if __name__ == "__main__":
    unittest.main()

