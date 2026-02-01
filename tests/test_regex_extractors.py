"""
Unit tests for regex_extractors module.
"""

import unittest
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.regex_extractors import parse_lease_term, parse_date, parse_word_number, parse_fractional_years, resolve_special_day


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

    def test_years_less_word_day(self):
        """Test: '999 years less one day from 25 December 1897'"""
        result = parse_lease_term("999 years less one day from 25 December 1897")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1897, 12, 25))
        self.assertEqual(result['expiry_date'], datetime(2896, 12, 24))  # 999 years minus 1 day
        self.assertEqual(result['tenure_years'], 999)

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

    def test_normalization_les_to_less(self):
        """Test normalization: 'les' -> 'less'"""
        result = parse_lease_term("99 years les 3 days from 25 March 1868")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1868, 3, 25))
        self.assertEqual(result['expiry_date'], datetime(1967, 3, 22))
        self.assertEqual(result['tenure_years'], 99)

    def test_normalization_rom_to_from(self):
        """Test normalization: 'rom' -> 'from'"""
        result = parse_lease_term("99 years rom 24 June 1862")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1862, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(1961, 6, 24))
        self.assertEqual(result['tenure_years'], 99)

    def test_normalization_special_chars(self):
        """Test normalization: remove special characters ´ ~ ¨"""
        result = parse_lease_term("99´ years from 24 June 1862")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1862, 6, 24))
        self.assertEqual(result['tenure_years'], 99)

    # --- New test cases for fractional years ---
    def test_fractional_years_three_quarters(self):
        """Test: '97 3/4 years from 25 March 1866'"""
        result = parse_lease_term("97 3/4 years from 25 March 1866")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1866, 3, 25))
        # 97.75 years = 97 years + 9 months
        self.assertEqual(result['expiry_date'], datetime(1963, 12, 25))
        self.assertEqual(result['tenure_years'], 97.75)

    def test_fractional_years_one_quarter(self):
        """Test: '54 1/4 years from 24 June 1898'"""
        result = parse_lease_term("54 1/4 years from 24 June 1898")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1898, 6, 24))
        # 54.25 years = 54 years + 3 months
        self.assertEqual(result['expiry_date'], datetime(1952, 9, 24))
        self.assertEqual(result['tenure_years'], 54.25)

    def test_fractional_years_three_quarters_september(self):
        """Test: '76 3/4 years from 29 September 1851'"""
        result = parse_lease_term("76 3/4 years from 29 September 1851")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1851, 9, 29))
        # 76.75 years = 76 years + 9 months
        self.assertEqual(result['expiry_date'], datetime(1928, 6, 29))
        self.assertEqual(result['tenure_years'], 76.75)

    def test_word_fraction_and_half(self):
        """Test: '65 and half years from 25 March 1904 determinable as therein mentioned'"""
        result = parse_lease_term("65 and half years from 25 March 1904 determinable as therein mentioned")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1904, 3, 25))
        # 65.5 years = 65 years + 6 months
        self.assertEqual(result['expiry_date'], datetime(1969, 9, 25))
        self.assertEqual(result['tenure_years'], 65.5)

    def test_word_fraction_and_a_half(self):
        """Test: '95 and a half years from 25 December 1868'"""
        result = parse_lease_term("95 and a half years from 25 December 1868")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1868, 12, 25))
        # 95.5 years = 95 years + 6 months
        self.assertEqual(result['expiry_date'], datetime(1964, 6, 25))
        self.assertEqual(result['tenure_years'], 95.5)

    def test_word_fraction_and_a_quarter_less_days(self):
        """Test: '52 and a quarter years less 10 days from 25 March 1906'"""
        result = parse_lease_term("52 and a quarter years less 10 days from 25 March 1906")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1906, 3, 25))
        # 52.25 years = 52 years + 3 months, then minus 10 days
        self.assertEqual(result['expiry_date'], datetime(1958, 6, 15))
        self.assertEqual(result['tenure_years'], 52.25)

    # --- New test cases for special day names ---
    def test_special_day_christmas(self):
        """Test: '99 years from Christmas Day 1900'"""
        result = parse_lease_term("99 years from Christmas Day 1900")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1900, 12, 25))
        self.assertEqual(result['expiry_date'], datetime(1999, 12, 25))
        self.assertEqual(result['tenure_years'], 99)

    def test_special_day_midsummer_less_days(self):
        """Test: '99 years less 10 days from Midsummer Day 1852'"""
        result = parse_lease_term("99 years less 10 days from Midsummer Day 1852")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1852, 6, 24))
        # 99 years minus 10 days
        self.assertEqual(result['expiry_date'], datetime(1951, 6, 14))
        self.assertEqual(result['tenure_years'], 99)

    def test_special_day_midsummer_parenthetical_less_days(self):
        """Test: '67 years (less 3 days) from Midsummer Day 1881'"""
        result = parse_lease_term("67 years (less 3 days) from Midsummer Day 1881")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1881, 6, 24))
        # 67 years minus 3 days
        self.assertEqual(result['expiry_date'], datetime(1948, 6, 21))
        self.assertEqual(result['tenure_years'], 67)

    # --- New test case for missing 'from' keyword ---
    def test_missing_from_keyword(self):
        """Test: '999 years 25 March 1896' (missing 'from')"""
        result = parse_lease_term("999 years 25 March 1896")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1896, 3, 25))
        self.assertEqual(result['expiry_date'], datetime(2895, 3, 25))
        self.assertEqual(result['tenure_years'], 999)

    # --- New test cases for additional patterns ---
    def test_years_less_months(self):
        """Test: '500 years less 9 months from 29 September 1585'"""
        result = parse_lease_term("500 years less 9 months from 29 September 1585")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1585, 9, 29))
        # 500 years minus 9 months = December 29, 2084
        self.assertEqual(result['expiry_date'], datetime(2084, 12, 29))
        self.assertEqual(result['tenure_years'], 500)

    def test_years_from_slash_date(self):
        """Test: '20 years from 28/06/1996'"""
        result = parse_lease_term("20 years from 28/06/1996")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1996, 6, 28))
        self.assertEqual(result['expiry_date'], datetime(2016, 6, 28))
        self.assertEqual(result['tenure_years'], 20)

    def test_years_less_the_last_days(self):
        """Test: '125 years (less the last seven days) from 25 December 2005'"""
        result = parse_lease_term("125 years (less the last seven days) from 25 December 2005")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2005, 12, 25))
        # 125 years minus 7 days
        self.assertEqual(result['expiry_date'], datetime(2130, 12, 18))
        self.assertEqual(result['tenure_years'], 125)

    def test_from_numeric_date_to_numeric_date(self):
        """Test: 'From 7.4.2006 to 1.9.2021'"""
        result = parse_lease_term("From 7.4.2006 to 1.9.2021")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2006, 4, 7))
        self.assertEqual(result['expiry_date'], datetime(2021, 9, 1))
        self.assertEqual(result['tenure_years'], 15)

    def test_date_to_date_without_from(self):
        """Test: '28 April 2006 to 24 December 2172'"""
        result = parse_lease_term("28 April 2006 to 24 December 2172")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2006, 4, 28))
        self.assertEqual(result['expiry_date'], datetime(2172, 12, 24))
        self.assertEqual(result['tenure_years'], 166)

    def test_missing_years_keyword(self):
        """Test: '999 from 27 April 2006' (missing 'years')"""
        result = parse_lease_term("999 from 27 April 2006")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2006, 4, 27))
        self.assertEqual(result['expiry_date'], datetime(3005, 4, 27))
        self.assertEqual(result['tenure_years'], 999)

    def test_from_numeric_to_text_date(self):
        """Test: 'from 30.3.2006 to 18 September 2126'"""
        result = parse_lease_term("from 30.3.2006 to 18 September 2126")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2006, 3, 30))
        self.assertEqual(result['expiry_date'], datetime(2126, 9, 18))
        self.assertEqual(result['tenure_years'], 120)

    def test_years_plus_days(self):
        """Test: '999 Years plus 7 days from 01 November 2004'"""
        result = parse_lease_term("999 Years plus 7 days from 01 November 2004")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2004, 11, 1))
        # 999 years plus 7 days
        self.assertEqual(result['expiry_date'], datetime(3003, 11, 8))
        self.assertEqual(result['tenure_years'], 999)

    def test_years_from_the(self):
        """Test: '999 years from the 22 December 1953'"""
        result = parse_lease_term("999 years from the 22 December 1953")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1953, 12, 22))
        self.assertEqual(result['expiry_date'], datetime(2952, 12, 22))
        self.assertEqual(result['tenure_years'], 999)

    def test_from_date_for_years(self):
        """Test: 'from and including 1 October 2002 for 20 years'"""
        result = parse_lease_term("from and including 1 October 2002 for 20 years")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2002, 10, 1))
        self.assertEqual(result['expiry_date'], datetime(2022, 10, 1))
        self.assertEqual(result['tenure_years'], 20)

    def test_normalization_jnuary_typo(self):
        """Test: '199 years (less 14 days) from 16 Jnuary 2006' (typo Jnuary)"""
        result = parse_lease_term("199 years (less 14 days) from 16 Jnuary 2006")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2006, 1, 16))
        # 199 years minus 14 days
        self.assertEqual(result['expiry_date'], datetime(2205, 1, 2))
        self.assertEqual(result['tenure_years'], 199)

    # --- New test cases for commencing and beginning patterns ---
    def test_years_and_days_commencing(self):
        """Test: '999 years and 10 days commencing on and including 10/5/2024'"""
        result = parse_lease_term("999 years and 10 days commencing on and including 10/5/2024")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2024, 5, 10))
        # 999 years plus 10 days
        self.assertEqual(result['expiry_date'], datetime(3023, 5, 20))
        self.assertEqual(result['tenure_years'], 999)

    def test_years_commencing_and_expiring(self):
        """Test: '189 years commencing on and including 01 September 1995 and expiring on and including 31 August 2184'"""
        result = parse_lease_term("189 years commencing on and including 01 September 1995 and expiring on and including 31 August 2184")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1995, 9, 1))
        self.assertEqual(result['expiry_date'], datetime(2184, 8, 31))
        self.assertEqual(result['tenure_years'], 189)

    def test_years_beginning_inclusive_ending_inclusive(self):
        """Test: '125 years beginning on 1 January 2013 inclusive and ending on 31 December 2138 inclusive'"""
        result = parse_lease_term("125 years beginning on 1 January 2013 inclusive and ending on 31 December 2138 inclusive")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2013, 1, 1))
        self.assertEqual(result['expiry_date'], datetime(2138, 12, 31))
        self.assertEqual(result['tenure_years'], 125)

    def test_years_beginning_on_and_including_no_end(self):
        """Test: '215 years beginning on and including 24 June 1988'"""
        result = parse_lease_term("215 years beginning on and including 24 June 1988")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1988, 6, 24))
        self.assertEqual(result['expiry_date'], datetime(2203, 6, 24))
        self.assertEqual(result['tenure_years'], 215)

    def test_years_commencing_and_ending(self):
        """Test: '22 years commencing on and including 8 November 2023 and ending on 7 November 2045'"""
        result = parse_lease_term("22 years commencing on and including 8 November 2023 and ending on 7 November 2045")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2023, 11, 8))
        self.assertEqual(result['expiry_date'], datetime(2045, 11, 7))
        self.assertEqual(result['tenure_years'], 22)

    def test_from_for_term_of_years_expiring(self):
        """Test: 'From and including 10 May 2013 for a term of years expiring on 9 December 2190'"""
        result = parse_lease_term("From and including 10 May 2013 for a term of years expiring on 9 December 2190")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2013, 5, 10))
        self.assertEqual(result['expiry_date'], datetime(2190, 12, 9))
        self.assertEqual(result['tenure_years'], 177)

    def test_commencing_for_term_of_years(self):
        """Test: 'commencing on 10 may 2013 for a term of 125 years'"""
        result = parse_lease_term("commencing on 10 may 2013 for a term of 125 years")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2013, 5, 10))
        self.assertEqual(result['expiry_date'], datetime(2138, 5, 10))
        self.assertEqual(result['tenure_years'], 125)

    def test_from_for_term_of_years_expiring_2(self):
        """Test: 'From and including 13 May 2013 for a term of years expiring on 9 December 2190'"""
        result = parse_lease_term("From and including 13 May 2013 for a term of years expiring on 9 December 2190")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2013, 5, 13))
        self.assertEqual(result['expiry_date'], datetime(2190, 12, 9))
        self.assertEqual(result['tenure_years'], 177)

    # --- New test cases for beginning/ending, commencing/expiring, etc. ---
    def test_beginning_ending_without_and(self):
        """Test: 'Beginning on and including 1 September 2016 ending on and including 2 August 3015'"""
        result = parse_lease_term("Beginning on and including 1 September 2016 ending on and including 2 August 3015")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 9, 1))
        self.assertEqual(result['expiry_date'], datetime(3015, 8, 2))
        self.assertEqual(result['tenure_years'], 998)

    def test_beginning_ending_comma_separated(self):
        """Test: 'beginning on and including 2 December 2016, ending on and including 1 December 2026'"""
        result = parse_lease_term("beginning on and including 2 December 2016, ending on and including 1 December 2026")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 12, 2))
        self.assertEqual(result['expiry_date'], datetime(2026, 12, 1))
        self.assertEqual(result['tenure_years'], 9)

    def test_word_years_beginning(self):
        """Test: 'Ten years beginning on and including 6 December 2016'"""
        result = parse_lease_term("Ten years beginning on and including 6 December 2016")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 12, 6))
        self.assertEqual(result['expiry_date'], datetime(2026, 12, 6))
        self.assertEqual(result['tenure_years'], 10)

    def test_a_term_commencing_expiring(self):
        """Test: 'A term commencing on and including 27 October 2016 and expiring on and including 23 October 2031'"""
        result = parse_lease_term("A term commencing on and including 27 October 2016 and expiring on and including 23 October 2031")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 10, 27))
        self.assertEqual(result['expiry_date'], datetime(2031, 10, 23))
        self.assertEqual(result['tenure_years'], 14)

    def test_years_on_and_from(self):
        """Test: '99 years on and from 1 June 2016'"""
        result = parse_lease_term("99 years on and from 1 June 2016")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 6, 1))
        self.assertEqual(result['expiry_date'], datetime(2115, 6, 1))
        self.assertEqual(result['tenure_years'], 99)

    def test_years_from_ordinal_date(self):
        """Test: '60 years from 1st June 1981'"""
        result = parse_lease_term("60 years from 1st June 1981")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(1981, 6, 1))
        self.assertEqual(result['expiry_date'], datetime(2041, 6, 1))
        self.assertEqual(result['tenure_years'], 60)

    def test_commencing_expiring_no_years(self):
        """Test: 'commencing on 28 July 2016 and expiring on 27 July 2115'"""
        result = parse_lease_term("commencing on 28 July 2016 and expiring on 27 July 2115")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 7, 28))
        self.assertEqual(result['expiry_date'], datetime(2115, 7, 27))
        self.assertEqual(result['tenure_years'], 98)

    def test_years_commencing_ordinal_date(self):
        """Test: '15 years commencing on and including 20th February 2015'"""
        result = parse_lease_term("15 years commencing on and including 20th February 2015")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2015, 2, 20))
        self.assertEqual(result['expiry_date'], datetime(2030, 2, 20))
        self.assertEqual(result['tenure_years'], 15)

    def test_years_less_days_beginning(self):
        """Test: '250 years less 20 days beginning on 18 October 2016'"""
        result = parse_lease_term("250 years less 20 days beginning on 18 October 2016")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2016, 10, 18))
        # 250 years minus 20 days
        self.assertEqual(result['expiry_date'], datetime(2266, 9, 28))
        self.assertEqual(result['tenure_years'], 250)

    # --- New test cases for starting, commencing from, expiring, up to ---
    def test_years_starting_and_ending(self):
        """Test: '125 years starting on 1 January 2019 and ending on 31 December 2144'"""
        result = parse_lease_term("125 years starting on 1 January 2019 and ending on 31 December 2144")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2019, 1, 1))
        self.assertEqual(result['expiry_date'], datetime(2144, 12, 31))
        self.assertEqual(result['tenure_years'], 125)

    def test_years_commencing_from_and_including(self):
        """Test: '999 years commencing from and including 13 September 2018'"""
        result = parse_lease_term("999 years commencing from and including 13 September 2018")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2018, 9, 13))
        self.assertEqual(result['expiry_date'], datetime(3017, 9, 13))
        self.assertEqual(result['tenure_years'], 999)

    def test_years_expiring_on(self):
        """Test: '147 years expiring on 23 June 2161'"""
        result = parse_lease_term("147 years expiring on 23 June 2161")

        self.assertIsNotNone(result)
        # Start date calculated by subtracting 147 years from expiry
        self.assertEqual(result['start_date'], datetime(2014, 6, 23))
        self.assertEqual(result['expiry_date'], datetime(2161, 6, 23))
        self.assertEqual(result['tenure_years'], 147)

    def test_years_expiring_on_2(self):
        """Test: '125 years expiring on 20 February 2125'"""
        result = parse_lease_term("125 years expiring on 20 February 2125")

        self.assertIsNotNone(result)
        # Start date calculated by subtracting 125 years from expiry
        self.assertEqual(result['start_date'], datetime(2000, 2, 20))
        self.assertEqual(result['expiry_date'], datetime(2125, 2, 20))
        self.assertEqual(result['tenure_years'], 125)

    def test_starting_and_ending_no_years(self):
        """Test: 'Starting on 20 December 2024 and ending on 19 December 2039'"""
        result = parse_lease_term("Starting on 20 December 2024 and ending on 19 December 2039")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2024, 12, 20))
        self.assertEqual(result['expiry_date'], datetime(2039, 12, 19))
        self.assertEqual(result['tenure_years'], 14)

    def test_years_from_and_including_the(self):
        """Test: '125 years from and including the 01 March 2023'"""
        result = parse_lease_term("125 years from and including the 01 March 2023")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2023, 3, 1))
        self.assertEqual(result['expiry_date'], datetime(2148, 3, 1))
        self.assertEqual(result['tenure_years'], 125)

    def test_from_up_to_and_including(self):
        """Test: 'From and including 12 August 2024 up to and including 30 September 2031'"""
        result = parse_lease_term("From and including 12 August 2024 up to and including 30 September 2031")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2024, 8, 12))
        self.assertEqual(result['expiry_date'], datetime(2031, 9, 30))
        self.assertEqual(result['tenure_years'], 7)

    def test_years_starting_on(self):
        """Test: '99 years starting on 3 December 2024'"""
        result = parse_lease_term("99 years starting on 3 December 2024")

        self.assertIsNotNone(result)
        self.assertEqual(result['start_date'], datetime(2024, 12, 3))
        self.assertEqual(result['expiry_date'], datetime(2123, 12, 3))
        self.assertEqual(result['tenure_years'], 99)


class TestParseFractionalYears(unittest.TestCase):
    """Tests for the parse_fractional_years helper function."""

    def test_parse_three_quarters(self):
        """Test parsing '97 3/4'."""
        result = parse_fractional_years("97 3/4")
        self.assertEqual(result, 97.75)

    def test_parse_one_quarter(self):
        """Test parsing '54 1/4'."""
        result = parse_fractional_years("54 1/4")
        self.assertEqual(result, 54.25)

    def test_parse_and_half(self):
        """Test parsing '65 and half'."""
        result = parse_fractional_years("65 and half")
        self.assertEqual(result, 65.5)

    def test_parse_and_a_half(self):
        """Test parsing '95 and a half'."""
        result = parse_fractional_years("95 and a half")
        self.assertEqual(result, 95.5)

    def test_parse_and_a_quarter(self):
        """Test parsing '52 and a quarter'."""
        result = parse_fractional_years("52 and a quarter")
        self.assertEqual(result, 52.25)

    def test_parse_plain_number(self):
        """Test parsing plain number '99'."""
        result = parse_fractional_years("99")
        self.assertEqual(result, 99.0)

    def test_parse_empty_string(self):
        """Test parsing empty string returns None."""
        result = parse_fractional_years("")
        self.assertIsNone(result)

    def test_parse_none(self):
        """Test parsing None returns None."""
        result = parse_fractional_years(None)
        self.assertIsNone(result)


class TestResolveSpecialDay(unittest.TestCase):
    """Tests for the resolve_special_day helper function."""

    def test_christmas_day(self):
        """Test resolving Christmas Day."""
        result = resolve_special_day("Christmas Day", "1900")
        self.assertEqual(result, datetime(1900, 12, 25))

    def test_christmas(self):
        """Test resolving Christmas."""
        result = resolve_special_day("Christmas", "1950")
        self.assertEqual(result, datetime(1950, 12, 25))

    def test_midsummer_day(self):
        """Test resolving Midsummer Day."""
        result = resolve_special_day("Midsummer Day", "1852")
        self.assertEqual(result, datetime(1852, 6, 24))

    def test_midsummer(self):
        """Test resolving Midsummer."""
        result = resolve_special_day("Midsummer", "1881")
        self.assertEqual(result, datetime(1881, 6, 24))

    def test_lady_day(self):
        """Test resolving Lady Day."""
        result = resolve_special_day("Lady Day", "1900")
        self.assertEqual(result, datetime(1900, 3, 25))

    def test_michaelmas(self):
        """Test resolving Michaelmas."""
        result = resolve_special_day("Michaelmas", "1900")
        self.assertEqual(result, datetime(1900, 9, 29))

    def test_michaelmas_day(self):
        """Test resolving Michaelmas Day."""
        result = resolve_special_day("Michaelmas Day", "1900")
        self.assertEqual(result, datetime(1900, 9, 29))

    def test_case_insensitive(self):
        """Test case insensitivity."""
        result = resolve_special_day("christmas day", "1900")
        self.assertEqual(result, datetime(1900, 12, 25))

    def test_unknown_day(self):
        """Test unknown day returns None."""
        result = resolve_special_day("Unknown Day", "1900")
        self.assertIsNone(result)

    def test_invalid_year(self):
        """Test invalid year returns None."""
        result = resolve_special_day("Christmas Day", "invalid")
        self.assertIsNone(result)

    def test_empty_inputs(self):
        """Test empty inputs return None."""
        self.assertIsNone(resolve_special_day("", "1900"))
        self.assertIsNone(resolve_special_day("Christmas Day", ""))
        self.assertIsNone(resolve_special_day(None, "1900"))
        self.assertIsNone(resolve_special_day("Christmas Day", None))


if __name__ == '__main__':
    unittest.main()

