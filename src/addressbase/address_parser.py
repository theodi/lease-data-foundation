"""Address parsing module using the postal library."""

import re

from postal.parser import parse_address

# Keywords that typically indicate building/house names
BUILDING_KEYWORDS = [
    "COURT",
    "LODGE",
    "HOUSE",
    "HALL",
    "MANOR",
    "TOWER",
    "TOWERS",
    "PLACE",
    "BUILDING",
    "BUILDINGS",
    "MANSION",
    "MANSIONS",
    "CHAMBERS",
    "ARCADE",
    "CENTRE",
    "CENTER",
]


def _extract_building_from_road(result: dict[str, str]) -> dict[str, str]:
    """
    Post-process the parsed result to extract building names from the road field.

    When libpostal doesn't properly separate building names (e.g., "35 ST KEYNA COURT")
    from street names, this function uses building keywords to split them.

    Examples of the default behavior:
    - "35 ST KEYNA COURT TEMPLE STREET" -> house_number: "35", road: "ST KEYNA COURT TEMPLE STREET"
    - 33, MILL GREEN LODGE RYLAND DRIVE, WITHAM CM8 1ZG -> house_number: "33", road: "MILL GREEN LODGE RYLAND DRIVE"

    Args:
        result: The parsed address dictionary from libpostal.

    Returns:
        The modified dictionary with 'house' extracted if a building keyword is found.
    """
    road = result.get("road", "")
    house_number = result.get("house_number", "")

    # If there's already a house component or no road, skip processing
    if "house" in result or not road:
        return result

    # Check if road contains a building keyword followed by more text (the actual street)
    for keyword in BUILDING_KEYWORDS:
        # Pattern: matches "BUILDING_NAME KEYWORD STREET_NAME"
        # e.g., "ST KEYNA COURT TEMPLE STREET" -> "ST KEYNA COURT" + "TEMPLE STREET"
        pattern = rf'^(.+?\s+{keyword})\s+(.+)$'
        match = re.match(pattern, road, re.IGNORECASE)
        if match:
            building_name = match.group(1).strip()
            street_name = match.group(2).strip()

            # Construct the house name with the house number if present
            if house_number:
                result["house"] = f"{house_number} {building_name}"
                # Remove house_number since it's now part of the house name
                del result["house_number"]
            else:
                result["house"] = building_name

            result["road"] = street_name
            return result

    return result


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
    result = {label: value.upper() for value, label in parsed}

    # Post-process to extract building names from road if needed
    result = _extract_building_from_road(result)

    return result
