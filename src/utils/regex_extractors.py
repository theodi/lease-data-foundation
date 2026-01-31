"""
Regex-based extractors for lease term parsing.

Extracts lease start date, end date, and tenure from various string formats.
"""

import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional, Dict, Any


def parse_date(day: str, month: str, year: str) -> Optional[datetime]:
    """
    Parse date components into a datetime object.

    Args:
        day: Day of the month
        month: Month name or number
        year: Year

    Returns:
        datetime object or None if parsing fails
    """
    try:
        # Try parsing with month name
        date_str = f"{day} {month} {year}"
        return datetime.strptime(date_str, "%d %B %Y")
    except ValueError:
        try:
            # Try abbreviated month name
            return datetime.strptime(date_str, "%d %b %Y")
        except ValueError:
            try:
                # Try numeric month (e.g., 29.9.1909)
                return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
            except ValueError:
                return None


def parse_word_number(word: str) -> Optional[int]:
    """
    Convert word numbers to integers.

    Args:
        word: Number as word (e.g., 'one', 'two') or digit string

    Returns:
        Integer value or None if parsing fails
    """
    word_to_num = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14,
        'fifteen': 15, 'sixteen': 16, 'seventeen': 17, 'eighteen': 18,
        'nineteen': 19, 'twenty': 20, 'thirty': 30, 'forty': 40,
        'fifty': 50, 'sixty': 60, 'seventy': 70, 'eighty': 80,
        'ninety': 90, 'hundred': 100
    }

    word_lower = word.lower().strip()

    # Check if it's a digit string (possibly with ~ or other chars)
    digits = re.sub(r'[^\d]', '', word)
    if digits:
        return int(digits)

    # Check word map
    if word_lower in word_to_num:
        return word_to_num[word_lower]

    return None


def parse_lease_term(term_str: str) -> Optional[Dict[str, Any]]:
    """
    Parse a lease term string to extract start date, expiry date, and tenure.

    Handles various formats including:
    - "99 years from 24 June 1862"
    - "99 years less 3 days from 25 March 1868"
    - "99 years from 29.9.1909"
    - "From and including 24 June 2020 to and including 23 June 2025"
    - "Beginning on and including 1 April 1982 and ending on and including 31 March 2197"
    - "a term of 10 years from and including 17 December 2021 to and including 16 December 2031"

    Args:
        term_str: The lease term string to parse

    Returns:
        Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
        or None if parsing fails
    """
    if not term_str:
        return None

    # Normalize whitespace (including non-breaking spaces)
    term_str = re.sub(r'[\s\u00A0]+', ' ', term_str.strip())

    # Pattern 1: "X years from and including DD Month YYYY to and including DD Month YYYY"
    # Example: "10 years from and including 25 August 2020 to and including 24 August 2030"
    # This must come before other patterns to capture stated years with explicit date ranges
    pattern1 = re.compile(
        r'(?:a\s+term\s+of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)\s*years?\s+'
        r'from\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'(?:to|until)\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern1.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))

        if start_date and expiry_date and years:
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 2: "X years from DD Month YYYY" (with optional modifiers like "less 3 days", "~", "renewable...")
    # Examples: "99 years from 24 June 1862", "99 years less 3 days from 25 March 1868"
    # Also handles: "215 years (less 3 days) from and including 24 June 1986"
    pattern2 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'(?:\s*\(?less\s+(\d+)\s+days?\)?)?'
        r'\s+from\s+(?:and\s+including\s+)?(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern2.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days = int(match.group(2)) if match.group(2) else 0
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years) - timedelta(days=less_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 3: "From [and including] DD Month YYYY to [and including] DD Month YYYY"
    # Examples: "From and including 24 June 2020 to and including 23 June 2025"
    pattern3 = re.compile(
        r'from\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'(?:to|until|ending\s+on|expiring\s+on|and\s+ending\s+on)\s+'
        r'(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern3.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))

        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': tenure_years,
                'source': 'regex'
            }

    # Pattern 4: "X years beginning on [and including] DD Month YYYY and ending on [and including] DD Month YYYY"
    # Example: "215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201"
    # This must come before pattern5 to capture stated years
    pattern4 = re.compile(
        r'(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)\s*years?\s+'
        r'beginning\s+on\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'and\s+ending\s+on\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern4.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))

        if start_date and expiry_date and years:
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 5: "Beginning on [and including] DD Month YYYY and ending on [and including] DD Month YYYY"
    # Example: "Beginning on and including 1 April 1982 and ending on and including 31 March 2197"
    pattern5 = re.compile(
        r'beginning\s+on\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'and\s+ending\s+on\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern5.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))

        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': tenure_years,
                'source': 'regex'
            }

    return None

