"""
Unit tests for regex_extractors module.
"""

import unittest
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.regex_extractors import parse_lease_term, parse_date, parse_word_number


class TestParseDateFunction(unittest.TestCase):
    """Tests for the parse_date helper function."""

    def test_parse_full_month_name(self):
        """Test parsing with full month name."""
        result = parse_date("24", "June", "1862")
        self.assertEqual(result, datetime(1862, 6, 24))

    def test_parse_abbreviated_month(self):
        """Test parsing with abbreviated month name."""
        result = parse_date("1", "Apr", "1982")
        self.assertEqual(result, datetime(1982, 4, 1))

    def test_parse_numeric_month(self):
        """Test parsing with numeric month (from date like 29.9.1909)."""
        result = parse_date("29", "9", "1909")
        self.assertEqual(result, datetime(1909, 9, 29))

    def test_parse_invalid_date(self):
        """Test parsing invalid date returns None."""
        result = parse_date("invalid", "invalid", "invalid")
        self.assertIsNone(result)


class TestParseWordNumberFunction(unittest.TestCase):
    """Tests for the parse_word_number helper function."""

    def test_parse_digit_string(self):
        """Test parsing digit strings."""
        self.assertEqual(parse_word_number("99"), 99)
        self.assertEqual(parse_word_number("10"), 10)

    def test_parse_digit_with_tilde(self):
        """Test parsing digits with special characters like ~."""
        self.assertEqual(parse_word_number("98~"), 98)

    def test_parse_word_numbers(self):
        """Test parsing word numbers."""
        self.assertEqual(parse_word_number("one"), 1)
        self.assertEqual(parse_word_number("ten"), 10)
        self.assertEqual(parse_word_number("Twenty"), 20)

    def test_parse_invalid_word(self):
        """Test parsing invalid word returns None."""
        self.assertIsNone(parse_word_number("invalid"))


class TestParseLeaseTerm(unittest.TestCase):
    """Tests for the main parse_lease_term function."""

    def test_years_from_date_basic(self):
        """Test: '99 years from 24 June 1862'"""
        result = parse_lease_term("99 years from 24 June 1862")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1862, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(1961, 6, 24))
        self.assertEqual(result['tenure_years'], 99)
        self.assertEqual(result['source'], 'regex')

    def test_years_less_days_from_date(self):
        """Test: '99 years less 3 days from 25 March 1868'"""
        result = parse_lease_term("99 years less 3 days from 25 March 1868")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1868, 3, 25))
        self.assertEqual(result['expiry_date'], datetime(1967, 3, 22))  # 99 years minus 3 days
        self.assertEqual(result['tenure_years'], 99)

    def test_years_from_numeric_date(self):
        """Test: '99 years from 29.9.1909'"""
        result = parse_lease_term("99 years from 29.9.1909")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1909, 9, 29))
        self.assertEqual(result['expiry_date'], datetime(2008, 9, 29))
        self.assertEqual(result['tenure_years'], 99)

    def test_years_from_september(self):
        """Test: '99 years from 29 September 1925'"""
        result = parse_lease_term("99 years from 29 September 1925")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1925, 9, 29))
        self.assertEqual(result['expiry_date'], datetime(2024, 9, 29))
        self.assertEqual(result['tenure_years'], 99)

    def test_years_with_tilde(self):
        """Test: '98~ years from 5 July 1931'"""
        result = parse_lease_term("98~ years from 5 July 1931")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1931, 7, 5))
        self.assertEqual(result['expiry_date'], datetime(2029, 7, 5))
        self.assertEqual(result['tenure_years'], 98)

    def test_years_with_renewable(self):
        """Test: '80 years from 29 September 1902 renewable as therein entioned'"""
        result = parse_lease_term("80 years from 29 September 1902 renewable as therein entioned")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1902, 9, 29))
        self.assertEqual(result['expiry_date'], datetime(1982, 9, 29))
        self.assertEqual(result['tenure_years'], 80)

    def test_from_to_including(self):
        """Test: 'From and including 24 June 2020 to and including 23 June 2025'"""
        result = parse_lease_term("From and including 24 June 2020 to and including 23 June 2025")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2020, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(2025, 6, 23))
        self.assertEqual(result['tenure_years'], 4)  # 4 full years + some months

    def test_years_from_to_including(self):
        """Test: '10 years from and including 25 August 2020 to and including 24 August 2030'"""
        result = parse_lease_term("10 years from and including 25 August 2020 to and including 24 August 2030")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2020, 8, 25))
        self.assertEqual(result['expiry_date'], datetime(2030, 8, 24))
        self.assertEqual(result['tenure_years'], 10)

    def test_word_year_from_to_including(self):
        """Test: 'one year from and including 6 June 2023 to and including 5 June 2024'"""
        result = parse_lease_term("one year from and including 6 June 2023 to and including 5 June 2024")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2023, 6, 6))
        self.assertEqual(result['expiry_date'], datetime(2024, 6, 5))
        self.assertEqual(result['tenure_years'], 1)

    def test_beginning_ending(self):
        """Test: 'Beginning on and including 1 April 1982 and ending on and including 31 March 2197'"""
        result = parse_lease_term("Beginning on and including 1 April 1982 and ending on and including 31 March 2197")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1982, 4, 1))
        self.assertEqual(result['expiry_date'], datetime(2197, 3, 31))
        self.assertEqual(result['tenure_years'], 214)  # 214 full years

    def test_term_of_years(self):
        """Test: 'a term of 10 years from and including 17 December 2021 to and including 16 December 2031'"""
        result = parse_lease_term("a term of 10 years from and including 17 December 2021 to and including 16 December 2031")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2021, 12, 17))
        self.assertEqual(result['expiry_date'], datetime(2031, 12, 16))
        self.assertEqual(result['tenure_years'], 10)

    def test_term_of_years_2(self):
        result = parse_lease_term("215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1986, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(2201, 6, 23))
        self.assertEqual(result['tenure_years'], 215)

    def test_term_of_years_3(self):
        result = parse_lease_term("215 years (less 3 days) from and including 24 June 1986")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1986, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(2201, 6, 21))
        self.assertEqual(result['tenure_years'], 215)

    def test_empty_string(self):
        """Test empty string returns None."""
        result = parse_lease_term("")
        self.assertIsNone(result)

    def test_none_input(self):
        """Test None input returns None."""
        result = parse_lease_term(None)
        self.assertIsNone(result)

    def test_invalid_format(self):
        """Test invalid format returns None."""
        result = parse_lease_term("This is not a lease term")
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()

