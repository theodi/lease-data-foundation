"""Address parsing module using the postal library."""

from postal.parser import parse_address


def parse_address_string(address: str) -> dict[str, str]:
    """
    Parse an address string into its components using libpostal.

    Args:
        address: The address string to parse.

    Returns:
        A dictionary mapping component labels to their values.
        Common labels include: house_number, road, city, postcode, etc.
    """
    parsed = parse_address(address)
    return {label: value.upper() for value, label in parsed}
