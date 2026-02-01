"""
Regex-based extractors for lease term parsing.

Extracts lease start date, end date, and tenure from various string formats.
"""

import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional, Dict, Any


# ============================================================================
# COMMON REGEX BUILDING BLOCKS (for maintainability and reuse)
# ============================================================================

# Word numbers commonly used in lease terms
WORD_NUMBERS = (
    r'one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|'
    r'fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|'
    r'fifty|sixty|seventy|eighty|ninety|hundred'
)

# Years number: digit or word (e.g., "99", "one", "999~")
YEARS_NUM = rf'(\d{{1,4}}|{WORD_NUMBERS})~?'

# Fractional years (e.g., "97 3/4", "65 and half", "52 and a quarter")
FRACTIONAL_YEARS = rf'(\d{{1,4}}(?:\s+\d+/\d+)?|\d{{1,4}}\s+and\s+(?:a\s+)?(?:half|quarter))~?'

# Date separator: space, period, or slash
DATE_SEP = r'[./\s]+'

# Day component (supports ordinal like 1st, 2nd handled in normalization)
DAY = r'(\d{1,2})'

# Month: text name or numeric
MONTH = r'([A-Za-z]+|\d{1,2})'

# Year: 4 digits
YEAR = r'(\d{4})'

# Full date pattern: DD sep Month sep YYYY
DATE_PATTERN = rf'{DAY}{DATE_SEP}{MONTH}{DATE_SEP}{YEAR}'

# Special day names (Christmas Day, Midsummer Day, Lady Day, Michaelmas)
SPECIAL_DAYS = r'(Christmas\s+Day|Midsummer\s+Day|Midsummer|Christmas|Lady\s+Day|Michaelmas(?:\s+Day)?)'

# Optional "and including" phrase
AND_INCLUDING = r'(?:and\s+including\s+)?'

# Optional "on" word
ON = r'(?:on\s+)?'

# Optional "the" word
THE = r'(?:the\s+)?'

# Start keywords: from, commencing, beginning, starting
START_KEYWORD = r'(?:from|commencing|beginning|starting)'

# Optional start prefix variations: "on and from", "from", "commencing on", "beginning on", "commencing from", "from and including"
START_PREFIX = rf'(?:{START_KEYWORD}\s+(?:{ON}|from\s+)?{AND_INCLUDING}|on\s+and\s+from\s+)'

# End keywords
END_KEYWORD = r'(?:to|until|up\s+to|ending|expiring|and\s+ending|and\s+expiring)'

# Days/months modifier: "less X days", "(less X days)", "plus X days", "and X days"
DAYS_WORD = rf'(\d+|{WORD_NUMBERS})'
MONTHS_WORD = rf'(\d+|{WORD_NUMBERS})'

# Optional "less/plus days" modifier
LESS_DAYS_MOD = rf'(?:\s*\(?less\s+{THE}?(?:last\s+)?{DAYS_WORD}\s+days?\)?)?'
PLUS_DAYS_MOD = rf'(?:\s+(?:plus|and)\s+{DAYS_WORD}\s+days?)?'
LESS_MONTHS_MOD = rf'(?:\s+less\s+{MONTHS_WORD}\s+months?)?'


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
    date_str = f"{day} {month} {year}"
    try:
        # Try parsing with month name
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


def _build_result(start_date: datetime, expiry_date: datetime, tenure_years) -> Dict[str, Any]:
    """Build the standard result dictionary."""
    return {
        'start_date': start_date,
        'expiry_date': expiry_date,
        'tenure_years': tenure_years,
        'source': 'regex'
    }


def _calculate_expiry(start_date: datetime, years: float, less_days: int = 0,
                      plus_days: int = 0, less_months: int = 0) -> datetime:
    """Calculate expiry date from start date and tenure adjustments."""
    full_years = int(years)
    fractional_months = int(round((years - full_years) * 12))
    expiry = start_date + relativedelta(years=full_years, months=fractional_months)
    expiry = expiry - timedelta(days=less_days) + timedelta(days=plus_days)
    expiry = expiry - relativedelta(months=less_months)
    return expiry


def parse_lease_term(term_str: str) -> Optional[Dict[str, Any]]:
    """
    Parse a lease term string to extract start date, expiry date, and tenure.

    Handles various formats including:
    - "99 years from 24 June 1862"
    - "99 years less 3 days from 25 March 1868"
    - "99 years from 29.9.1909" (numeric date)
    - "From and including 24 June 2020 to and including 23 June 2025"
    - "Beginning on and including 1 April 1982 and ending on and including 31 March 2197"
    - "a term of 10 years from and including 17 December 2021 to and including 16 December 2031"
    - "97 3/4 years from 25 March 1866" (fractional years)
    - "65 and half years from 25 March 1904" (word fractions)
    - "99 years from Christmas Day 1900" (special day names)
    - "999 years 25 March 1896" (missing 'from' keyword)
    - "999 years and 10 days commencing on and including 10/5/2024"
    - "commencing on 28 July 2016 and expiring on 27 July 2115"

    Args:
        term_str: The lease term string to parse

    Returns:
        Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
        or None if parsing fails
    """
    if not term_str:
        return None

    term_str = normalise_term_str(term_str)

    # ========================================================================
    # PATTERN GROUP 1: Years with both start and end dates explicitly stated
    # ========================================================================

    # Pattern 1: "X years [from|commencing|beginning] ... [to|ending|expiring] ..."
    # Examples:
    #   "10 years from and including 25 August 2020 to and including 24 August 2030"
    #   "215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201"
    #   "189 years commencing on and including 01 September 1995 and expiring on and including 31 August 2184"
    #   "125 years beginning on 1 January 2013 inclusive and ending on 31 December 2138 inclusive"
    #   "22 years commencing on and including 8 November 2023 and ending on 7 November 2045"
    pattern_years_start_end = re.compile(
        rf'(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'{START_KEYWORD}\s+{ON}{AND_INCLUDING}{DATE_PATTERN}\s*(?:inclusive\s+)?'
        rf'(?:and\s+)?{END_KEYWORD}\s+{ON}{AND_INCLUDING}{DATE_PATTERN}\s*(?:inclusive)?',
        re.IGNORECASE
    )

    match = pattern_years_start_end.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        expiry_date = parse_date(match.group(5), match.group(6), match.group(7))
        if start_date and expiry_date and years:
            return _build_result(start_date, expiry_date, years)

    # ========================================================================
    # PATTERN GROUP 2: Date range without explicit years (tenure calculated)
    # ========================================================================

    # Pattern 2a: "From [and including] DD Month YYYY to/until/ending DD Month YYYY"
    # Also: "Beginning on ... ending on ..." and "commencing on ... expiring on ..."
    # Examples:
    #   "From and including 24 June 2020 to and including 23 June 2025"
    #   "Beginning on and including 1 April 1982 and ending on and including 31 March 2197"
    #   "Beginning on and including 1 September 2016 ending on and including 2 August 3015"
    #   "commencing on 28 July 2016 and expiring on 27 July 2115"
    #   "A term commencing on and including 27 October 2016 and expiring on and including 23 October 2031"
    pattern_date_range = re.compile(
        rf'(?:a\s+term\s+)?{START_KEYWORD}\s+{ON}{AND_INCLUDING}{DATE_PATTERN}\s*[,]?\s*'
        rf'(?:and\s+)?{END_KEYWORD}\s+{ON}{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_date_range.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return _build_result(start_date, expiry_date, tenure_years)

    # Pattern 2b: "From DD.MM.YYYY to DD.MM.YYYY" or mixed "from DD.MM.YYYY to DD Month YYYY"
    pattern_numeric_date_range = re.compile(
        rf'from\s+{DATE_PATTERN}\s+'
        rf'to\s+{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_numeric_date_range.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return _build_result(start_date, expiry_date, tenure_years)

    # Pattern 2c: "DD Month YYYY to/until DD Month YYYY" (without "from")
    # Example: "5 June 2002 until 31 December 3001"
    pattern_date_to_date = re.compile(
        rf'^{DATE_PATTERN}\s+(?:to|until)\s+{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_date_to_date.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return _build_result(start_date, expiry_date, tenure_years)

    # Pattern 2d: "From and including DD Month YYYY for a term of years expiring on DD Month YYYY"
    pattern_for_term_expiring = re.compile(
        rf'from\s+{AND_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+a\s+term\s+(?:of\s+)?(?:years?\s+)?expiring\s+{ON}{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_for_term_expiring.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = relativedelta(expiry_date, start_date).years
            return _build_result(start_date, expiry_date, tenure_years)

    # ========================================================================
    # PATTERN GROUP 3: Years with modifiers (less/plus days/months) + start date
    # ========================================================================

    # Pattern 3a: Fractional years with optional "less days" and date/special day
    # Examples:
    #   "97 3/4 years from 25 March 1866"
    #   "65 and half years from 25 March 1904"
    #   "52 and a quarter years less 10 days from 25 March 1906"
    #   "99 years less 10 days from Midsummer Day 1852"
    #   "67 years (less 3 days) from Midsummer Day 1881"
    #   "215 years (less 3 days) from and including 24 June 1986"
    pattern_fractional_years = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{FRACTIONAL_YEARS}\s*years?'
        rf'{LESS_DAYS_MOD}'
        rf'\s+{START_PREFIX}{THE}?'
        rf'(?:{DATE_PATTERN}|{SPECIAL_DAYS}\s+{YEAR})',
        re.IGNORECASE
    )

    match = pattern_fractional_years.search(term_str)
    if match:
        years_str = match.group(1)
        years_float = parse_fractional_years(years_str)
        less_days_str = match.group(2)
        less_days = parse_word_number(less_days_str) if less_days_str else 0

        # Check if regular date or special day
        if match.group(3):  # Regular date (day, month, year)
            start_date = parse_date(match.group(3), match.group(4), match.group(5))
        else:  # Special day name
            special_day = match.group(6)
            year = match.group(7)
            start_date = resolve_special_day(special_day, year)

        if years_float and start_date:
            expiry_date = _calculate_expiry(start_date, years_float, less_days=less_days)
            return _build_result(start_date, expiry_date, years_float)

    # Pattern 3b: Years with "less months"
    # Example: "500 years less 9 months from 29 September 1585"
    pattern_less_months = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?'
        rf'\s+less\s+{MONTHS_WORD}\s+months?'
        rf'\s+{START_PREFIX}{THE}?{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_less_months.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_months = parse_word_number(match.group(2))
        start_date = parse_date(match.group(3), match.group(4), match.group(5))
        if years and start_date and less_months:
            expiry_date = _calculate_expiry(start_date, years, less_months=less_months)
            return _build_result(start_date, expiry_date, years)

    # Pattern 3c: Years with "plus days" or "and X days"
    # Examples:
    #   "999 Years plus 7 days from 01 November 2004"
    #   "999 years and 10 days commencing on and including 10/5/2024"
    pattern_plus_days = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?'
        rf'\s+(?:plus|and)\s+{DAYS_WORD}\s+days?'
        rf'\s+{START_PREFIX}{THE}?{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_plus_days.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        plus_days = parse_word_number(match.group(2))
        start_date = parse_date(match.group(3), match.group(4), match.group(5))
        if years and start_date and plus_days is not None:
            expiry_date = _calculate_expiry(start_date, years, plus_days=plus_days)
            return _build_result(start_date, expiry_date, years)

    # Pattern 3d: Years with "less days" using beginning/commencing
    # Example: "250 years less 20 days beginning on 18 October 2016"
    pattern_less_days_beginning = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?'
        rf'\s+less\s+{DAYS_WORD}\s+days?'
        rf'\s+(?:beginning|commencing)\s+{ON}{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_less_days_beginning.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        less_days = parse_word_number(match.group(2))
        start_date = parse_date(match.group(3), match.group(4), match.group(5))
        if years and start_date and less_days is not None:
            expiry_date = _calculate_expiry(start_date, years, less_days=less_days)
            return _build_result(start_date, expiry_date, years)

    # ========================================================================
    # PATTERN GROUP 4: Simple years + start date (no modifiers)
    # ========================================================================

    # Pattern 4a: "X years [from|commencing|beginning|on and from] [and including] [the] DD Month YYYY"
    # Examples:
    #   "99 years from 24 June 1862"
    #   "999 years from the 22 December 1953"
    #   "20 years from 28/06/1996"
    #   "99 years on and from 1 June 2016"
    #   "215 years beginning on and including 24 June 1988"
    #   "15 years commencing on and including 20th February 2015"
    #   "Ten years beginning on and including 6 December 2016" (word number)
    #   "125 years from and including the 01 March 2023"
    pattern_years_from_date = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'{START_PREFIX}{THE}?{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_from_date.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4b: "X years from Special Day YYYY"
    # Example: "99 years from Christmas Day 1900"
    pattern_years_special_day = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'{START_PREFIX}{THE}?'
        rf'{SPECIAL_DAYS}\s+{YEAR}',
        re.IGNORECASE
    )

    match = pattern_years_special_day.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        special_day = match.group(2)
        year = match.group(3)
        start_date = resolve_special_day(special_day, year)
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4c: "[commencing|beginning] on DD Month YYYY for a term of X years"
    # Example: "commencing on 10 may 2013 for a term of 125 years"
    pattern_commencing_for_term = re.compile(
        rf'(?:commencing|beginning)\s+{ON}{AND_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?',
        re.IGNORECASE
    )

    match = pattern_commencing_for_term.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4d: "from and including DD Month YYYY for X years"
    # Example: "from and including 1 October 2002 for 20 years"
    pattern_from_for_years = re.compile(
        rf'from\s+{AND_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+{YEARS_NUM}\s*years?',
        re.IGNORECASE
    )

    match = pattern_from_for_years.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4e: "X years expiring on DD Month YYYY" (only expiry date given, calculate start)
    # Examples: "147 years expiring on 23 June 2161", "125 years expiring on 20 February 2125"
    pattern_years_expiring = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'expiring\s+{ON}{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_expiring.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        expiry_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and expiry_date:
            # Calculate start date by subtracting years from expiry
            start_date = expiry_date - relativedelta(years=years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4f: "X years to [and including] DD Month YYYY" (years with expiry only)
    # Example: "15 years to and including 9 December 2039"
    pattern_years_to_date = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'to\s+{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_to_date.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        expiry_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and expiry_date:
            # Calculate start date by subtracting years from expiry
            start_date = expiry_date - relativedelta(years=years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4g: "From DD Month YYYY for a term of X years"
    # Example: "From 25 May 1988 for a term of 212 years"
    pattern_from_date_for_term = re.compile(
        rf'from\s+{AND_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?',
        re.IGNORECASE
    )

    match = pattern_from_date_for_term.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4h: "commencing on DD Month YYYY and expiring X years thereafter"
    # Example: "Commences on 28 July 2024 and expires 50 years thereafter"
    pattern_date_years_thereafter = re.compile(
        rf'{START_KEYWORD}\s+{ON}{AND_INCLUDING}{DATE_PATTERN}\s+'
        rf'and\s+(?:expiring|expiry)\s+{YEARS_NUM}\s*years?\s+thereafter',
        re.IGNORECASE
    )

    match = pattern_date_years_thereafter.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4i: "X years and Y months from [and including] DD Month YYYY"
    # Examples: "31 years and 6 months from 28 March 2024", "20 years and 3 months from and including 9 September 2015"
    pattern_years_and_months = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'and\s+{MONTHS_WORD}\s+months?\s+'
        rf'{START_PREFIX}{THE}?{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_and_months.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        months = parse_word_number(match.group(2))
        start_date = parse_date(match.group(3), match.group(4), match.group(5))
        if years and start_date and months is not None:
            expiry_date = start_date + relativedelta(years=years, months=months)
            return _build_result(start_date, expiry_date, years)

    # ========================================================================
    # PATTERN GROUP 5: Fallback patterns (missing keywords)
    # ========================================================================

    # Pattern 5a: "X years DD Month YYYY" (missing 'from')
    # Example: "999 years 25 March 1896"
    pattern_years_date_no_from = re.compile(
        rf'^(?:a\s+term\s+of\s+)?{YEARS_NUM}\s*years?\s+'
        rf'{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_date_no_from.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 5b: "X from DD Month YYYY" (missing "years")
    # Example: "999 from 27 April 2006"
    pattern_num_from_date = re.compile(
        rf'^(\d{{1,4}})\s+from\s+{THE}?{AND_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_num_from_date.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # ========================================================================
    # FALLBACK: Remove parenthetical text and retry
    # ========================================================================
    # If all patterns failed and there's text in parentheses, remove it and retry
    # Example: "99 years (renewable) from 24 June 1862" -> "99 years from 24 June 1862"
    term_without_parens = re.sub(r'\s*\([^)]*\)', '', term_str).strip()
    if term_without_parens != term_str:
        return parse_lease_term(term_without_parens)

    return None


def normalise_term_str(term_str: str) -> str:
    """
    Normalise lease term string for parsing by removing extra whitespace and fixing common issues.
    :param term_str: the input lease term string
    :return: normalised lease term string
    """
    term_str = re.sub(r'[\s\u00A0]+', ' ', term_str.strip())

    # Remove problematic special characters (but keep colons for date formats like 12:7:1973)
    term_str = term_str.replace('´', '').replace('~', '').replace('¨', '').replace(',', '')

    # Remove "Residue of" prefix
    term_str = re.sub(r'^Residue\s+of\s+', '', term_str, flags=re.IGNORECASE)

    # Remove "midnight on" phrases
    term_str = term_str.replace(" midnight on", " ")

    # Remove ordinal suffixes from dates (1st -> 1, 2nd -> 2, etc.)
    term_str = re.sub(r'\b(\d{1,2})(?:st|nd|rd|th)\b', r'\1', term_str, flags=re.IGNORECASE)

    # Remove "of" between day and month (e.g., "1 of January" -> "1 January")
    term_str = re.sub(r'\b(\d{1,2})\s+of\s+([A-Za-z]+)\b', r'\1 \2', term_str, flags=re.IGNORECASE)

    # Fix "including on" -> "including" (duplicate "on")
    term_str = re.sub(r'\bincluding\s+on\b', 'including', term_str, flags=re.IGNORECASE)

    # Fix "to and expiring" -> "to" or "expiring" (redundant)
    term_str = re.sub(r'\bto\s+and\s+expiring\b', 'expiring', term_str, flags=re.IGNORECASE)

    # Fix "an including" -> "and including" (typo)
    term_str = re.sub(r'\ban\s+including\b', 'and including', term_str, flags=re.IGNORECASE)

    # Fix "beginning in," -> "beginning on" (typo)
    term_str = re.sub(r'\bbeginning\s+in\b', 'beginning on', term_str, flags=re.IGNORECASE)

    # Fix "ending on," -> "ending on" (extra comma already removed above)
    # Fix "Commences" -> "commencing", "expires" -> "expiring"
    term_str = re.sub(r'\bCommences\b', 'commencing', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bexpires\b', 'expiring', term_str, flags=re.IGNORECASE)

    # Remove colons after From/To (e.g., "From:" -> "From", "To:" -> "to")
    term_str = re.sub(r'\bFrom\s*:', 'From', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bTo\s*:', 'to', term_str, flags=re.IGNORECASE)

    # Convert colon date separators to dots (e.g., "12:7:1973" -> "12.7.1973")
    term_str = re.sub(r'\b(\d{1,2}):(\d{1,2}):(\d{4})\b', r'\1.\2.\3', term_str)

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

