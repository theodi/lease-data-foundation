"""Unit tests for the address parser module."""

import unittest

from src.addressbase.address_parser import parse_address_string


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
        """Test that parse_address_string returns a dictionary."""
        address = "3B BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)
        self.assertEqual(result["house_number"], "3B")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address2(self):
        """Test that parse_address_string returns a dictionary."""
        address = "FLAT 2, 2 BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "FLAT 2")
        self.assertEqual(result["house_number"], "2")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address3(self):
        """Test that parse_address_string returns a dictionary."""
        address = "UNIT B1, 2 BELSHAM STREET, LONDON E9 6NG"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "UNIT B1")
        self.assertEqual(result["house_number"], "2")
        self.assertEqual(result["road"], "BELSHAM STREET")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "E9 6NG")

    def test_address4(self):
        """Test that parse_address_string returns a dictionary."""
        # address = "GARDEN FLAT, 2 TASKER ROAD, LONDON NW3 2YR"
        address = "2 TASKER ROAD, LONDON NW3 2YR"
        result = parse_address_string(address)
        print(result)

        self.assertEqual(result["unit"], "GARDEN FLAT")
        self.assertEqual(result["house_number"], "2")
        self.assertEqual(result["road"], "TASKER ROAD")
        self.assertEqual(result["city"], "LONDON")
        self.assertEqual(result["postcode"], "NW3 2YR")



if __name__ == "__main__":
    unittest.main()
