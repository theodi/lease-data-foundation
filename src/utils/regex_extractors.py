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


def parse_fractional_years(years_str: str) -> Optional[float]:
    """
    Parse years string that may contain fractions.

    Handles formats like:
    - "97 3/4" -> 97.75
    - "54 1/4" -> 54.25
    - "65 and half" -> 65.5
    - "95 and a half" -> 95.5
    - "52 and a quarter" -> 52.25
    - "99" -> 99.0

    Args:
        years_str: The years string to parse

    Returns:
        Float value of years or None if parsing fails
    """
    if not years_str:
        return None

    years_str = years_str.strip().lower()

    # Handle "X and a half" / "X and half" / "X and a quarter"
    and_fraction_pattern = re.compile(
        r'^(\d+)\s+and\s+(?:a\s+)?(half|quarter)$',
        re.IGNORECASE
    )
    match = and_fraction_pattern.match(years_str)
    if match:
        base = int(match.group(1))
        fraction = match.group(2).lower()
        if fraction == 'half':
            return base + 0.5
        elif fraction == 'quarter':
            return base + 0.25

    # Handle "X Y/Z" format (e.g., "97 3/4")
    fraction_pattern = re.compile(r'^(\d+)\s+(\d+)/(\d+)$')
    match = fraction_pattern.match(years_str)
    if match:
        base = int(match.group(1))
        numerator = int(match.group(2))
        denominator = int(match.group(3))
        if denominator != 0:
            return base + (numerator / denominator)

    # Handle plain numbers (including word numbers)
    num = parse_word_number(years_str)
    if num is not None:
        return float(num)

    return None


def resolve_special_day(day_name: str, year: str) -> Optional[datetime]:
    """
    Resolve special day names like Christmas Day and Midsummer Day to actual dates.

    Args:
        day_name: The special day name (e.g., "Christmas Day", "Midsummer Day")
        year: The year as a string

    Returns:
        datetime object or None if not a recognized special day
    """
    if not day_name or not year:
        return None

    day_name_lower = day_name.lower().strip()
    try:
        year_int = int(year)
    except ValueError:
        return None

    special_days = {
        'christmas day': (12, 25),
        'christmas': (12, 25),
        'midsummer day': (6, 24),  # Traditional Midsummer Day in England
        'midsummer': (6, 24),
        'lady day': (3, 25),  # Feast of the Annunciation
        'michaelmas': (9, 29),  # Feast of St. Michael
        'michaelmas day': (9, 29),
    }

    if day_name_lower in special_days:
        month, day = special_days[day_name_lower]
        return datetime(year_int, month, day)

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
    - "97 3/4 years from 25 March 1866" (fractional years)
    - "65 and half years from 25 March 1904" (word fractions)
    - "99 years from Christmas Day 1900" (special day names)
    - "999 years 25 March 1896" (missing 'from' keyword)

    Args:
        term_str: The lease term string to parse

    Returns:
        Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
        or None if parsing fails
    """
    if not term_str:
        return None

    term_str = normalise_term_str(term_str)

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

    # Pattern 2a: Fractional/word-fraction years with "less X days" and special days
    # Examples: "97 3/4 years from 25 March 1866", "65 and half years from 25 March 1904"
    # Also: "52 and a quarter years less 10 days from 25 March 1906"
    # Also: "99 years less 10 days from Midsummer Day 1852"
    # Also: "67 years (less 3 days) from Midsummer Day 1881"
    pattern2a = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}(?:\s+\d+/\d+)?|\d{1,4}\s+and\s+(?:a\s+)?(?:half|quarter))~?\s*years?'
        r'(?:\s*\(?less\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+days?\)?)?'
        r'\s+from\s+(?:and\s+including\s+)?'
        r'(?:(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})|'
        r'(Christmas\s+Day|Midsummer\s+Day|Midsummer|Christmas|Lady\s+Day|Michaelmas(?:\s+Day)?)\s+(\d{4}))',
        re.IGNORECASE
    )

    match = pattern2a.search(term_str)
    if match:
        years_str = match.group(1)
        years_float = parse_fractional_years(years_str)
        less_days_str = match.group(2)
        less_days = parse_word_number(less_days_str) if less_days_str else 0

        # Check if regular date or special day
        if match.group(3):  # Regular date (day, month, year)
            day, month, year = match.group(3), match.group(4), match.group(5)
            start_date = parse_date(day, month, year)
        else:  # Special day name
            special_day = match.group(6)
            year = match.group(7)
            start_date = resolve_special_day(special_day, year)

        if years_float and start_date:
            # Convert fractional years to months
            full_years = int(years_float)
            fractional_months = int(round((years_float - full_years) * 12))
            expiry_date = start_date + relativedelta(years=full_years, months=fractional_months) - timedelta(days=less_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years_float,
                'source': 'regex'
            }

    # Pattern 2: "X years from DD Month YYYY" (with optional modifiers like "less 3 days", "~", "renewable...")
    # Examples: "99 years from 24 June 1862", "99 years less 3 days from 25 March 1868"
    # Also handles: "215 years (less 3 days) from and including 24 June 1986"
    # Also handles: "999 years less one day from 25 December 1897"
    pattern2 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'(?:\s*\(?less\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+days?\)?)?'
        r'\s+from\s+(?:and\s+including\s+)?(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern2.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days_str = match.group(2)
        less_days = parse_word_number(less_days_str) if less_days_str else 0
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

    # Pattern 2b: "X years from Special Day YYYY" (e.g., "99 years from Christmas Day 1900")
    pattern2b = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'(?:\s*\(?less\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+days?\)?)?'
        r'\s+from\s+(?:and\s+including\s+)?'
        r'(Christmas\s+Day|Midsummer\s+Day|Midsummer|Christmas|Lady\s+Day|Michaelmas(?:\s+Day)?)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern2b.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days_str = match.group(2)
        less_days = parse_word_number(less_days_str) if less_days_str else 0
        special_day = match.group(3)
        year = match.group(4)
        start_date = resolve_special_day(special_day, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years) - timedelta(days=less_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 2c: "X years DD Month YYYY" (missing 'from' keyword)
    # Example: "999 years 25 March 1896"
    pattern2c = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'\s+(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern2c.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
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

    # Pattern 6: "X years less Y months from DD Month YYYY"
    # Example: "500 years less 9 months from 29 September 1585"
    pattern6 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'\s+less\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+months?'
        r'\s+from\s+(?:the\s+)?(?:and\s+including\s+)?(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern6.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_months = parse_word_number(match.group(2))
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date and less_months:
            expiry_date = start_date + relativedelta(years=years) - relativedelta(months=less_months)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 7: "X years (less the last Y days) from DD Month YYYY"
    # Example: "125 years (less the last seven days) from 25 December 2005"
    pattern7 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'\s*\(less\s+(?:the\s+)?(?:last\s+)?(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen)\s+days?\)'
        r'\s+from\s+(?:the\s+)?(?:and\s+including\s+)?(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern7.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days = parse_word_number(match.group(2))
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date and less_days:
            expiry_date = start_date + relativedelta(years=years) - timedelta(days=less_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 8: "X years plus Y days from DD Month YYYY"
    # Example: "999 Years plus 7 days from 01 November 2004"
    pattern8 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'\s+plus\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+days?'
        r'\s+from\s+(?:the\s+)?(?:and\s+including\s+)?(\d{1,2})[.\s/]+([A-Za-z]+|\d{1,2})[.\s/]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern8.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        plus_days = parse_word_number(match.group(2))
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date and plus_days:
            expiry_date = start_date + relativedelta(years=years) + timedelta(days=plus_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 9: "X years from the DD Month YYYY" or "X years from DD/MM/YYYY"
    # Examples: "999 years from the 22 December 1953", "20 years from 28/06/1996"
    pattern9 = re.compile(
        r'^(?:a term of\s+)?(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)~?\s*years?'
        r'\s+from\s+(?:the\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern9.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 10: "From DD.MM.YYYY to DD.MM.YYYY" or "From DD/MM/YYYY to DD/MM/YYYY" (numeric date ranges)
    # Examples: "From 7.4.2006 to 1.9.2021", "from 30.3.2006 to 18 September 2126"
    pattern10 = re.compile(
        r'from\s+(\d{1,2})[./](\d{1,2})[./](\d{4})\s+'
        r'to\s+(?:(\d{1,2})[./](\d{1,2})[./](\d{4})|(\d{1,2})\s+([A-Za-z]+)\s+(\d{4}))',
        re.IGNORECASE
    )

    match = pattern10.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))

        # Check if end date is numeric or text format
        if match.group(4):  # Numeric end date
            expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        else:  # Text end date (e.g., "18 September 2126")
            expiry_date = parse_date(match.group(7), match.group(8), match.group(9))

        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': tenure_years,
                'source': 'regex'
            }

    # Pattern 11: "DD Month YYYY to DD Month YYYY" (without "from")
    # Example: "28 April 2006 to 24 December 2172"
    pattern11 = re.compile(
        r'^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'to\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE
    )

    match = pattern11.search(term_str)
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

    # Pattern 12: "X from DD Month YYYY" (missing "years" keyword)
    # Example: "999 from 27 April 2006"
    pattern12 = re.compile(
        r'^(\d{1,4})\s+from\s+(?:the\s+)?(?:and\s+including\s+)?(\d{1,2})[.\s]+([A-Za-z]+|\d{1,2})[.\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern12.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 13: "from and including DD Month YYYY for X years"
    # Example: "from and including 1 October 2002 for 20 years"
    pattern13 = re.compile(
        r'from\s+(?:and\s+including\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+'
        r'for\s+(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)\s*years?',
        re.IGNORECASE
    )

    match = pattern13.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))

        if start_date and years:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 14: "X years and Y days commencing on [and including] DD/MM/YYYY"
    # Example: "999 years and 10 days commencing on and including 10/5/2024"
    pattern14 = re.compile(
        r'(\d{1,4})\s*years?\s+and\s+(\d+)\s*days?\s+'
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern14.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        plus_days = parse_word_number(match.group(2))
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date and plus_days is not None:
            expiry_date = start_date + relativedelta(years=years) + timedelta(days=plus_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 15: "X years commencing on [and including] DD Month YYYY and expiring on [and including] DD Month YYYY"
    # Example: "189 years commencing on and including 01 September 1995 and expiring on and including 31 August 2184"
    pattern15 = re.compile(
        r'(\d{1,4})\s*years?\s+'
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'and\s+(?:expiring|ending)\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern15.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))

        if years and start_date and expiry_date:
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 16: "X years beginning on DD Month YYYY inclusive and ending on DD Month YYYY inclusive"
    # Example: "125 years beginning on 1 January 2013 inclusive and ending on 31 December 2138 inclusive"
    pattern16 = re.compile(
        r'(\d{1,4})\s*years?\s+'
        r'beginning\s+(?:on\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s*(?:inclusive)?\s+'
        r'and\s+ending\s+(?:on\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s*(?:inclusive)?',
        re.IGNORECASE
    )

    match = pattern16.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))

        if years and start_date and expiry_date:
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 17: "X years beginning on [and including] DD Month YYYY" (no end date, calculate from years)
    # Example: "215 years beginning on and including 24 June 1988"
    pattern17 = re.compile(
        r'(\d{1,4})\s*years?\s+'
        r'beginning\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})(?:\s|$)',
        re.IGNORECASE
    )

    match = pattern17.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 18: "X years commencing on [and including] DD Month YYYY and ending on DD Month YYYY"
    # Example: "22 years commencing on and including 8 November 2023 and ending on 7 November 2045"
    pattern18 = re.compile(
        r'(\d{1,4})\s*years?\s+'
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'and\s+ending\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern18.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))

        if years and start_date and expiry_date:
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 19: "From and including DD Month YYYY for a term of years expiring on DD Month YYYY"
    # Example: "From and including 10 May 2013 for a term of years expiring on 9 December 2190"
    pattern19 = re.compile(
        r'from\s+(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'for\s+a\s+term\s+(?:of\s+)?(?:years?\s+)?expiring\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern19.search(term_str)
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

    # Pattern 20: "commencing on DD Month YYYY for a term of X years"
    # Example: "commencing on 10 may 2013 for a term of 125 years"
    pattern20 = re.compile(
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'for\s+a\s+term\s+(?:of\s+)?(\d{1,4})\s*years?',
        re.IGNORECASE
    )

    match = pattern20.search(term_str)
    if match:
        day, month, year = match.group(1), match.group(2), match.group(3)
        start_date = parse_date(day, month, year)
        years = parse_word_number(match.group(4))

        if start_date and years:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 21: "X years commencing on [and including] DD Month YYYY" (no end date)
    # Example: "125 years commencing on and including 1 January 2013"
    pattern21 = re.compile(
        r'(\d{1,4})\s*years?\s+'
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})(?:\s|$)',
        re.IGNORECASE
    )

    match = pattern21.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 22: "Beginning on and including DD Month YYYY ending on and including DD Month YYYY"
    # Also handles comma-separated: "beginning on and including 2 December 2016, ending on and including 1 December 2026"
    # Example: "Beginning on and including 1 September 2016 ending on and including 2 August 3015"
    pattern22 = re.compile(
        r'beginning\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s*'
        r'(?:and\s+)?ending\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern22.search(term_str)
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

    # Pattern 23: "X years beginning on [and including] DD Month YYYY" (word numbers like "Ten years")
    # Example: "Ten years beginning on and including 6 December 2016"
    pattern23 = re.compile(
        r'(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|'
        r'sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred)\s*years?\s+'
        r'beginning\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern23.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 24: "A term commencing on [and including] DD Month YYYY and expiring on [and including] DD Month YYYY"
    # Example: "A term commencing on and including 27 October 2016 and expiring on and including 23 October 2031"
    pattern24 = re.compile(
        r'(?:a\s+)?term\s+commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'and\s+expiring\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern24.search(term_str)
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

    # Pattern 25: "X years on and from DD Month YYYY"
    # Example: "99 years on and from 1 June 2016"
    pattern25 = re.compile(
        r'(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)\s*years?\s+'
        r'on\s+and\s+from\s+(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern25.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 26: "commencing on DD Month YYYY and expiring on DD Month YYYY" (without years)
    # Example: "commencing on 28 July 2016 and expiring on 27 July 2115"
    pattern26 = re.compile(
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})\s+'
        r'and\s+expiring\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern26.search(term_str)
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

    # Pattern 27: "X years less Y days beginning on DD Month YYYY"
    # Example: "250 years less 20 days beginning on 18 October 2016"
    pattern27 = re.compile(
        r'(\d{1,4}|one|two|three|four|five|six|seven|eight|nine|ten)\s*years?\s+'
        r'less\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\s*days?\s+'
        r'beginning\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern27.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days = parse_word_number(match.group(2))
        day, month, year = match.group(3), match.group(4), match.group(5)
        start_date = parse_date(day, month, year)

        if years and start_date and less_days is not None:
            expiry_date = start_date + relativedelta(years=years) - timedelta(days=less_days)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    # Pattern 28: "X years commencing on [and including] DD Month YYYY" (word numbers)
    # Example: "fifteen years commencing on and including 20 February 2015"
    pattern28 = re.compile(
        r'(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|'
        r'sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred)\s*years?\s+'
        r'commencing\s+(?:on\s+)?(?:and\s+including\s+)?(\d{1,2})[./\s]+([A-Za-z]+|\d{1,2})[./\s]+(\d{4})',
        re.IGNORECASE
    )

    match = pattern28.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        day, month, year = match.group(2), match.group(3), match.group(4)
        start_date = parse_date(day, month, year)

        if years and start_date:
            expiry_date = start_date + relativedelta(years=years)
            return {
                'start_date': start_date,
                'expiry_date': expiry_date,
                'tenure_years': years,
                'source': 'regex'
            }

    return None


def normalise_term_str(term_str: str) -> str:
    """
    Normalise lease term string for parsing by removing extra whitespace and fixing common issues.
    :param term_str: the input lease term string
    :return: normalised lease term string
    """
    term_str = re.sub(r'[\s\u00A0]+', ' ', term_str.strip())

    # Remove problematic special characters
    term_str = term_str.replace('´', '').replace('~', '').replace('¨', '').replace(',', '')

    # Remove ordinal suffixes from dates (1st -> 1, 2nd -> 2, etc.)
    term_str = re.sub(r'\b(\d{1,2})(?:st|nd|rd|th)\b', r'\1', term_str, flags=re.IGNORECASE)

    # Fix common spelling errors
    term_str = re.sub(r'\bles\b', 'less', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\brom\b', 'from', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bfrm\b', 'from', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bJanuaryu\b', 'January', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bJnuary\b', 'January', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bFeburary\b', 'February', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bFebuary\b', 'February', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bSeptmber\b', 'September', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bNovmber\b', 'November', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bDecmber\b', 'December', term_str, flags=re.IGNORECASE)
    return term_str

