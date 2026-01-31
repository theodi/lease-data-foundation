"""
Unit tests for lease_term_validator module.

Uses the same test data from test_regex_extractors.py TestParseLeaseTerm.
"""

import unittest
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.lease_term_validator import (
    validate_lease_term,
    is_lease_term_valid,
    LeaseTermValidationResult,
    LeaseTermValidationError
)
from src.utils.regex_extractors import parse_lease_term


class TestLeaseTermValidationResult(unittest.TestCase):
    """Tests for LeaseTermValidationResult class."""

    def test_empty_result_is_valid(self):
        """Test that a new result with no errors is valid."""
        result = LeaseTermValidationResult()
        self.assertTrue(result.is_valid)

    def test_result_with_error_is_invalid(self):
        """Test that a result with an error is invalid."""
        result = LeaseTermValidationResult()
        result.add_error("TEST_ERROR", "Test error message")
        self.assertFalse(result.is_valid)

    def test_result_with_warning_is_valid(self):
        """Test that a result with only warnings is still valid."""
        result = LeaseTermValidationResult()
        result.add_warning("TEST_WARNING", "Test warning message")
        self.assertTrue(result.is_valid)


class TestValidateLeaseTermBasic(unittest.TestCase):
    """Tests for basic validation scenarios."""

    def test_none_input(self):
        """Test validation of None input."""
        result = validate_lease_term(None)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors[0].code, "NULL_DATA")

    def test_missing_start_date(self):
        """Test validation with missing start_date."""
        data = {'expiry_date': datetime(2025, 1, 1), 'tenure_years': 10}
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "MISSING_FIELD" for e in result.errors))

    def test_missing_expiry_date(self):
        """Test validation with missing expiry_date."""
        data = {'start_date': datetime(2015, 1, 1), 'tenure_years': 10}
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "MISSING_FIELD" for e in result.errors))

    def test_missing_tenure_years(self):
        """Test validation with missing tenure_years."""
        data = {'start_date': datetime(2015, 1, 1), 'expiry_date': datetime(2025, 1, 1)}
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "MISSING_FIELD" for e in result.errors))

    def test_invalid_date_order(self):
        """Test validation when start_date is after expiry_date."""
        data = {
            'start_date': datetime(2030, 1, 1),
            'expiry_date': datetime(2020, 1, 1),
            'tenure_years': 10
        }
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "INVALID_DATE_ORDER" for e in result.errors))

    def test_negative_tenure(self):
        """Test validation with negative tenure_years."""
        data = {
            'start_date': datetime(2015, 1, 1),
            'expiry_date': datetime(2025, 1, 1),
            'tenure_years': -5
        }
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "INVALID_TENURE" for e in result.errors))

    def test_zero_tenure(self):
        """Test validation with zero tenure_years."""
        data = {
            'start_date': datetime(2015, 1, 1),
            'expiry_date': datetime(2025, 1, 1),
            'tenure_years': 0
        }
        result = validate_lease_term(data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(e.code == "INVALID_TENURE" for e in result.errors))


class TestValidateLeaseTermWarnings(unittest.TestCase):
    """Tests for validation warnings."""

    def test_tenure_mismatch_warning(self):
        """Test warning when tenure calculation doesn't match expiry date."""
        data = {
            'start_date': datetime(2015, 1, 1),
            'expiry_date': datetime(2025, 6, 1),  # 10 years + 5 months off
            'tenure_years': 10
        }
        result = validate_lease_term(data)
        self.assertTrue(result.is_valid)  # Still valid, just a warning
        self.assertTrue(any(w.code == "TENURE_MISMATCH" for w in result.warnings))

    def test_future_start_date_warning(self):
        """Test warning when start_date is in the future."""
        data = {
            'start_date': datetime(2030, 1, 1),
            'expiry_date': datetime(2040, 1, 1),
            'tenure_years': 10
        }
        result = validate_lease_term(data, reference_date=datetime(2025, 1, 1))
        self.assertTrue(result.is_valid)
        self.assertTrue(any(w.code == "FUTURE_START_DATE" for w in result.warnings))

    def test_expired_lease_warning(self):
        """Test warning when lease has expired."""
        data = {
            'start_date': datetime(2000, 1, 1),
            'expiry_date': datetime(2010, 1, 1),
            'tenure_years': 10
        }
        result = validate_lease_term(data, reference_date=datetime(2025, 1, 1))
        self.assertTrue(result.is_valid)
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))


class TestValidateLeaseTermFromRegex(unittest.TestCase):
    """Tests using data from test_regex_extractors.py TestParseLeaseTerm."""

    # Reference date for validation (current date context: January 31, 2026)
    REFERENCE_DATE = datetime(2026, 1, 31)

    def test_years_from_date_basic(self):
        """Test: '99 years from 24 June 1862'"""
        lease_data = parse_lease_term("99 years from 24 June 1862")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_less_days_from_date(self):
        """Test: '99 years less 3 days from 25 March 1868'"""
        lease_data = parse_lease_term("99 years less 3 days from 25 March 1868")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_from_numeric_date(self):
        """Test: '99 years from 29.9.1909'"""
        lease_data = parse_lease_term("99 years from 29.9.1909")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_from_september(self):
        """Test: '99 years from 29 September 1925'"""
        lease_data = parse_lease_term("99 years from 29 September 1925")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired (expires Sept 2024, reference is Jan 2026)
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_with_tilde(self):
        """Test: '98~ years from 5 July 1931'"""
        lease_data = parse_lease_term("98~ years from 5 July 1931")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Expires July 2029, still active
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_with_renewable(self):
        """Test: '80 years from 29 September 1902 renewable as therein entioned'"""
        lease_data = parse_lease_term("80 years from 29 September 1902 renewable as therein entioned")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_from_to_including(self):
        """Test: 'From and including 24 June 2020 to and including 23 June 2025'"""
        lease_data = parse_lease_term("From and including 24 June 2020 to and including 23 June 2025")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # tenure_years is 4 (calculated), but actual span is ~5 years, may have mismatch warning
        # This lease has expired (June 2025 < Jan 2026)
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_years_from_to_including(self):
        """Test: '10 years from and including 25 August 2020 to and including 24 August 2030'"""
        lease_data = parse_lease_term("10 years from and including 25 August 2020 to and including 24 August 2030")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Still active
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_word_year_from_to_including(self):
        """Test: 'one year from and including 6 June 2023 to and including 5 June 2024'"""
        lease_data = parse_lease_term("one year from and including 6 June 2023 to and including 5 June 2024")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # This lease has expired
        self.assertTrue(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_beginning_ending(self):
        """Test: 'Beginning on and including 1 April 1982 and ending on and including 31 March 2197'"""
        lease_data = parse_lease_term("Beginning on and including 1 April 1982 and ending on and including 31 March 2197")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Still active, very long lease
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_term_of_years(self):
        """Test: 'a term of 10 years from and including 17 December 2021 to and including 16 December 2031'"""
        lease_data = parse_lease_term("a term of 10 years from and including 17 December 2021 to and including 16 December 2031")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Still active
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_term_of_years_2(self):
        """Test: '215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201'"""
        lease_data = parse_lease_term("215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Still active
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))

    def test_term_of_years_3(self):
        """Test: '215 years (less 3 days) from and including 24 June 1986'"""
        lease_data = parse_lease_term("215 years (less 3 days) from and including 24 June 1986")
        result = validate_lease_term(lease_data, reference_date=self.REFERENCE_DATE)

        self.assertTrue(result.is_valid)
        # Still active
        self.assertFalse(any(w.code == "LEASE_EXPIRED" for w in result.warnings))


class TestIsLeaseTermValid(unittest.TestCase):
    """Tests for the is_lease_term_valid convenience function."""

    def test_valid_lease(self):
        """Test that a valid lease returns True."""
        lease_data = parse_lease_term("10 years from and including 25 August 2020 to and including 24 August 2030")
        self.assertTrue(is_lease_term_valid(lease_data))

    def test_invalid_lease(self):
        """Test that None returns False."""
        self.assertFalse(is_lease_term_valid(None))

    def test_invalid_date_order(self):
        """Test that invalid date order returns False."""
        data = {
            'start_date': datetime(2030, 1, 1),
            'expiry_date': datetime(2020, 1, 1),
            'tenure_years': 10
        }
        self.assertFalse(is_lease_term_valid(data))


if __name__ == '__main__':
    unittest.main()

