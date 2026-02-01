"""
Unit tests for T5 extractor module.
"""

import unittest
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.t5_extractor import T5LeaseExtractor, parse_lease_term_t5, get_extractor


class TestT5ExtractorParseDateMethod(unittest.TestCase):
    """Tests for the _parse_date helper method."""

    @classmethod
    def setUpClass(cls):
        """Set up the extractor once for all tests."""
        cls.extractor = T5LeaseExtractor.__new__(T5LeaseExtractor)
        cls.extractor.model_path = "./t5_model/trained_t5"
        cls.extractor._tokenizer = None
        cls.extractor._model = None
        cls.extractor._max_length = 64

    def test_parse_date_dd_mm_yyyy_slash(self):
        """Test parsing DD/MM/YYYY format."""
        result = self.extractor._parse_date("24/06/1862")
        self.assertEqual(result, datetime(1862, 6, 24))

    def test_parse_date_dd_mm_yyyy_dot(self):
        """Test parsing DD.MM.YYYY format."""
        result = self.extractor._parse_date("25.03.1868")
        self.assertEqual(result, datetime(1868, 3, 25))

    def test_parse_date_dd_mm_yyyy_dash(self):
        """Test parsing DD-MM-YYYY format."""
        result = self.extractor._parse_date("29-09-1909")
        self.assertEqual(result, datetime(1909, 9, 29))

    def test_parse_date_not_specified(self):
        """Test that 'Not specified' returns None."""
        result = self.extractor._parse_date("Not specified")
        self.assertIsNone(result)

    def test_parse_date_empty_string(self):
        """Test that empty string returns None."""
        result = self.extractor._parse_date("")
        self.assertIsNone(result)

    def test_parse_date_none(self):
        """Test that None input returns None."""
        result = self.extractor._parse_date(None)
        self.assertIsNone(result)

    def test_parse_date_christmas_day(self):
        """Test parsing Christmas Day special date."""
        result = self.extractor._parse_date("Christmas Day 1900")
        self.assertEqual(result, datetime(1900, 12, 25))

    def test_parse_date_midsummer_day(self):
        """Test parsing Midsummer Day special date."""
        result = self.extractor._parse_date("Midsummer Day 1895")
        self.assertEqual(result, datetime(1895, 6, 24))

    def test_parse_date_michaelmas(self):
        """Test parsing Michaelmas special date."""
        result = self.extractor._parse_date("Michaelmas 1920")
        self.assertEqual(result, datetime(1920, 9, 29))


class TestT5ExtractorParseTenureMethod(unittest.TestCase):
    """Tests for the _parse_tenure helper method."""

    @classmethod
    def setUpClass(cls):
        """Set up the extractor once for all tests."""
        cls.extractor = T5LeaseExtractor.__new__(T5LeaseExtractor)
        cls.extractor.model_path = "./t5_model/trained_t5"
        cls.extractor._tokenizer = None
        cls.extractor._model = None
        cls.extractor._max_length = 64

    def test_parse_tenure_simple_years(self):
        """Test parsing '99 years' format."""
        result = self.extractor._parse_tenure("99 years")
        self.assertEqual(result, 99)

    def test_parse_tenure_999_years(self):
        """Test parsing '999 years' format."""
        result = self.extractor._parse_tenure("999 years")
        self.assertEqual(result, 999)

    def test_parse_tenure_singular_year(self):
        """Test parsing '1 year' format."""
        result = self.extractor._parse_tenure("1 year")
        self.assertEqual(result, 1)

    def test_parse_tenure_with_less_days(self):
        """Test parsing '25 years less 3 days' - should return base years."""
        result = self.extractor._parse_tenure("25 years less 3 days")
        self.assertEqual(result, 25)

    def test_parse_tenure_not_specified(self):
        """Test that 'Not specified' returns None."""
        result = self.extractor._parse_tenure("Not specified")
        self.assertIsNone(result)

    def test_parse_tenure_empty(self):
        """Test that empty string returns None."""
        result = self.extractor._parse_tenure("")
        self.assertIsNone(result)

    def test_parse_tenure_none(self):
        """Test that None input returns None."""
        result = self.extractor._parse_tenure(None)
        self.assertIsNone(result)


class TestT5ExtractorParseT5Output(unittest.TestCase):
    """Tests for the _parse_t5_output method."""

    @classmethod
    def setUpClass(cls):
        """Set up the extractor once for all tests."""
        cls.extractor = T5LeaseExtractor.__new__(T5LeaseExtractor)
        cls.extractor.model_path = "./t5_model/trained_t5"
        cls.extractor._tokenizer = None
        cls.extractor._model = None
        cls.extractor._max_length = 64

    def test_parse_output_start_and_tenure(self):
        """Test parsing output with start date and tenure (typical format)."""
        # Format: "24/06/1862Not specified99 years"
        result = self.extractor._parse_t5_output("24/06/1862Not specified99 years")
        self.assertEqual(result['start_date'], datetime(1862, 6, 24))
        self.assertEqual(result['tenure_years'], 99)
        # Expiry should be calculated
        self.assertEqual(result['expiry_date'], datetime(1961, 6, 24))

    def test_parse_output_two_dates(self):
        """Test parsing output with two dates (from-to format)."""
        # Format: "25/08/202024/08/2030Not specified"
        result = self.extractor._parse_t5_output("25/08/202024/08/2030Not specified")
        self.assertEqual(result['start_date'], datetime(2020, 8, 25))
        self.assertEqual(result['expiry_date'], datetime(2030, 8, 24))
        # Tenure should be calculated
        self.assertEqual(result['tenure_years'], 10)

    def test_parse_output_two_dates_with_tenure(self):
        """Test parsing output with two dates and explicit tenure."""
        # Format: "17/12/202116/12/203110 years"
        result = self.extractor._parse_t5_output("17/12/202116/12/203110 years")
        self.assertEqual(result['start_date'], datetime(2021, 12, 17))
        self.assertEqual(result['expiry_date'], datetime(2031, 12, 16))
        self.assertEqual(result['tenure_years'], 10)

    def test_parse_output_special_day(self):
        """Test parsing output with special day like Christmas Day."""
        # Format: "Christmas Day 1900Not specified99 years"
        result = self.extractor._parse_t5_output("Christmas Day 1900Not specified99 years")
        self.assertEqual(result['start_date'], datetime(1900, 12, 25))
        self.assertEqual(result['tenure_years'], 99)
        self.assertEqual(result['expiry_date'], datetime(1999, 12, 25))

    def test_parse_output_empty(self):
        """Test parsing empty output."""
        result = self.extractor._parse_t5_output("")
        self.assertIsNone(result['start_date'])
        self.assertIsNone(result['expiry_date'])
        self.assertIsNone(result['tenure_years'])

    def test_parse_output_none(self):
        """Test parsing None output."""
        result = self.extractor._parse_t5_output(None)
        self.assertIsNone(result['start_date'])
        self.assertIsNone(result['expiry_date'])
        self.assertIsNone(result['tenure_years'])


class TestT5ExtractorParseDolDate(unittest.TestCase):
    """Tests for the _parse_dol_date helper method."""

    @classmethod
    def setUpClass(cls):
        """Set up the extractor once for all tests."""
        cls.extractor = T5LeaseExtractor.__new__(T5LeaseExtractor)
        cls.extractor.model_path = "./t5_model/trained_t5"
        cls.extractor._tokenizer = None
        cls.extractor._model = None
        cls.extractor._max_length = 64

    def test_parse_dol_dash_format(self):
        """Test parsing DD-MM-YYYY format."""
        result = self.extractor._parse_dol_date("25-03-1868")
        self.assertEqual(result, datetime(1868, 3, 25))

    def test_parse_dol_slash_format(self):
        """Test parsing DD/MM/YYYY format."""
        result = self.extractor._parse_dol_date("25/03/1868")
        self.assertEqual(result, datetime(1868, 3, 25))

    def test_parse_dol_dot_format(self):
        """Test parsing DD.MM.YYYY format."""
        result = self.extractor._parse_dol_date("25.03.1868")
        self.assertEqual(result, datetime(1868, 3, 25))

    def test_parse_dol_empty(self):
        """Test that empty string returns None."""
        result = self.extractor._parse_dol_date("")
        self.assertIsNone(result)

    def test_parse_dol_none(self):
        """Test that None input returns None."""
        result = self.extractor._parse_dol_date(None)
        self.assertIsNone(result)


class TestT5ExtractorExtractMethod(unittest.TestCase):
    """Integration tests for the extract method using mocked model."""

    def setUp(self):
        """Set up mocked extractor for each test."""
        self.extractor = T5LeaseExtractor.__new__(T5LeaseExtractor)
        self.extractor.model_path = "./t5_model/trained_t5"
        self.extractor._tokenizer = MagicMock()
        self.extractor._model = MagicMock()
        self.extractor._max_length = 64

    def _mock_model_output(self, raw_output: str):
        """Helper to set up mock for a specific output."""
        # Mock tokenizer
        mock_input = MagicMock()
        mock_input.input_ids = MagicMock()
        self.extractor._tokenizer.return_value = mock_input
        self.extractor._tokenizer.decode.return_value = raw_output

        # Mock model
        self.extractor._model.generate.return_value = [MagicMock()]

    def test_extract_years_from_date(self):
        """Test extracting '99 years from 24 June 1862'."""
        self._mock_model_output("24/06/1862Not specified99 years")

        result = self.extractor.extract("99 years from 24 June 1862")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1862, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(1961, 6, 24))
        self.assertEqual(result['tenure_years'], 99)
        self.assertEqual(result['extractor'], 't5')

    def test_extract_from_to_dates(self):
        """Test extracting 'From 25 August 2020 to 24 August 2030'."""
        self._mock_model_output("25/08/202024/08/2030Not specified")

        result = self.extractor.extract("From 25 August 2020 to 24 August 2030")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2020, 8, 25))
        self.assertEqual(result['expiry_date'], datetime(2030, 8, 24))
        self.assertEqual(result['tenure_years'], 10)
        self.assertEqual(result['extractor'], 't5')

    def test_extract_empty_string(self):
        """Test that empty string returns None."""
        result = self.extractor.extract("")
        self.assertIsNone(result)

    def test_extract_none(self):
        """Test that None input returns None."""
        result = self.extractor.extract(None)
        self.assertIsNone(result)

    def test_extract_whitespace_only(self):
        """Test that whitespace-only string returns None."""
        result = self.extractor.extract("   ")
        self.assertIsNone(result)

    def test_extract_insufficient_data(self):
        """Test that insufficient data (model returns garbage) returns None."""
        self._mock_model_output("Residential")

        result = self.extractor.extract("125 years")

        # Should return None because there's no valid start/end date or tenure
        self.assertIsNone(result)

    def test_extract_with_dol_parameter(self):
        """Test extracting with DOL parameter when start date is missing."""
        self._mock_model_output("999 years")

        result = self.extractor.extract("999 years from the date of the lease", dol="25-03-1868")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1868, 3, 25))
        self.assertEqual(result['tenure_years'], 999)
        self.assertEqual(result['expiry_date'], datetime(2867, 3, 25))
        self.assertEqual(result['extractor'], 't5')

    def test_extract_christmas_day(self):
        """Test extracting with special day name."""
        self._mock_model_output("Christmas Day 1900Not specified99 years")

        result = self.extractor.extract("99 years from Christmas Day 1900")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1900, 12, 25))
        self.assertEqual(result['tenure_years'], 99)
        self.assertEqual(result['expiry_date'], datetime(1999, 12, 25))
        self.assertEqual(result['extractor'], 't5')


class TestGlobalExtractor(unittest.TestCase):
    """Tests for the global extractor instance and convenience function."""

    def test_get_extractor_returns_same_instance(self):
        """Test that get_extractor returns the same instance on repeated calls."""
        # Note: This modifies global state so may need cleanup
        import src.utils.t5_extractor as module

        # Reset global state
        module._extractor = None

        extractor1 = get_extractor()
        extractor2 = get_extractor()

        self.assertIs(extractor1, extractor2)

        # Cleanup
        module._extractor = None

    def test_parse_lease_term_t5_convenience_function(self):
        """Test that parse_lease_term_t5 convenience function is accessible."""
        # Just verify the function exists and is callable
        self.assertTrue(callable(parse_lease_term_t5))


if __name__ == '__main__':
    unittest.main()

