"""Unit tests for the address parser module."""

import unittest

from src.addressbase.address_parser import parse_address_string
from src.addressbase.match_addresses import extract_base_number


class TestExtractBaseNumber(unittest.TestCase):
    """Test cases for the extract_base_number function."""

    def test_simple_number(self):
        """Test extracting base from simple number."""
        self.assertEqual(extract_base_number("1"), "1")
        self.assertEqual(extract_base_number("85"), "85")
        self.assertEqual(extract_base_number("153"), "153")

    def test_number_with_letter_suffix(self):
        """Test extracting base from number with letter suffix."""
        self.assertEqual(extract_base_number("85A"), "85")
        self.assertEqual(extract_base_number("1A"), "1")
        self.assertEqual(extract_base_number("3B"), "3")
        self.assertEqual(extract_base_number("7C"), "7")

    def test_range_number(self):
        """Test extracting base from range."""
        self.assertEqual(extract_base_number("153-157"), "153")
        self.assertEqual(extract_base_number("1-3"), "1")
        self.assertEqual(extract_base_number("10-20"), "10")

    def test_range_with_suffix(self):
        """Test extracting base from range with suffix."""
        self.assertEqual(extract_base_number("1A-1B"), "1")


class TestAddressParser(unittest.TestCase):
    """Test cases for the parse_address_string function."""

    def test_parse_7b_agnes_street(self):
        """Test parsing '7B AGNES STREET, LONDON E14 7DG'."""
        address = "7 AGNES STREET, LONDON E14 7DG"
        result = parse_address_string(address)

        self.assertIn("house_number", result)
        self.assertIn("road", result)
        self.assertIn("city", result)
        self.assertIn("postcode", result)
        self.assertEqual(result["house_number"], "7")
        self.assertEqual(result["road"], "AGNES STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E14 7DG")

    def test_parse_7a_agnes_street(self):
        """Test parsing '7A AGNES STREET, LONDON E14 7DG'."""
        address = "7A AGNES STREET, LONDON E14 7DG"
        result = parse_address_string(address)

        self.assertIn("house_number", result)
        self.assertIn("road", result)
        self.assertIn("city", result)
        self.assertIn("postcode", result)
        self.assertEqual(result["house_number"], "7A")
        self.assertEqual(result["road"], "AGNES STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E14 7DG")

    def test_parse_flat_10_swan_court(self):
        """Test parsing 'FLAT 10, SWAN COURT, 10 AGNES STREET, LONDON E14 7DG'."""
        address = "FLAT 10, SWAN COURT, 10 AGNES STREET, LONDON E14 7DG"
        result = parse_address_string(address)

        self.assertEqual(result["unit"], "FLAT 10")
        self.assertEqual(result["house"], "SWAN COURT")
        self.assertEqual(result["house_number"], "10")
        self.assertEqual(result["road"], "AGNES STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E14 7DG")

    def test_parse_flat_1_agnes_street(self):
        """Test parsing 'FLAT 1, 1 AGNES STREET, LONDON E14 7DG'."""
        address = "FLAT 1, 1 AGNES STREET, LONDON E14 7DG"
        result = parse_address_string(address)

        print(result)
        self.assertEqual(result["unit"], "FLAT 1")
        self.assertEqual(result["house_number"], "1")
        self.assertEqual(result["road"], "AGNES STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E14 7DG")

    def test_address1(self):
        address = "3B BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)
        self.assertEqual(result["house_number"], "3B")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address2(self):
        address = "FLAT 2, 2 BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "FLAT 2")
        self.assertEqual(result["house_number"], "2")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address3(self):
        address = "UNIT B1, 2 BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "UNIT B1")
        self.assertEqual(result["house_number"], "2")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address4(self):
        # address = "GROUND FLOOR SHOP PREMISES, TIME & LIFE BUILDING, 153-157 NEW BOND STREET, LONDON W1S 2TY"
        address = "TIME & LIFE BUILDING, 153-157 NEW BOND STREET, LONDON W1S 2TY"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["house"], "TIME & LIFE BUILDING")
        self.assertEqual(result["house_number"], "153-157")
        self.assertEqual(result["road"], "NEW BOND STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "W1S 2TY")

    def test_court_parsing(self):
        address = "FLAT 10, SWAN COURT, 10 AGNES STREET, LONDON E14 7DG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "FLAT 10")
        self.assertEqual(result["house"], "SWAN COURT")
        self.assertEqual(result["house_number"], "10")
        self.assertEqual(result["road"], "AGNES STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E14 7DG")

    def test_court_parsing2(self):
        address = "35 ST KEYNA COURT TEMPLE STREET, KEYNSHAM, BRISTOL BS31 1HB"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["house"], "35 ST KEYNA COURT")
        self.assertEqual(result["road"], "TEMPLE STREET")
        self.assertEqual(result["postcode"], "BS31 1HB")

    def test_lodge_parsing3(self):
        address = "33, MILL GREEN LODGE RYLAND DRIVE, WITHAM CM8 1ZG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["house"], "33 MILL GREEN LODGE")
        self.assertEqual(result["road"], "RYLAND DRIVE")
        self.assertEqual(result["postcode"], "CM8 1ZG")



    def test_address5(self):
        """Test that parse_address_string returns a dictionary."""
        address = "4 MOXON STREET, LONDON W1U 4EW"
        result = parse_address_string(address)
        print(result)

if __name__ == "__main__":
    unittest.main()
