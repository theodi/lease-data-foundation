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

# Word numbers commonly used in lease terms (up to 100)
WORD_NUMBERS = (
    r'one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|'
    r'fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|'
    r'fifty|sixty|seventy|eighty|ninety|hundred'
)

# Number pattern: digits (with optional comma for thousands) or word numbers
# Matches: "99", "999~", "10,000", "one", "twenty"
NUM = rf'(?:\d{{1,6}}(?:,\d{{3}})?|{WORD_NUMBERS})~?'

# Captured number (for years, days, months)
NUM_CAP = rf'({NUM})'

# Fractional years: "97 3/4", "65 and half", "52 and a quarter", or plain number
# Group captures the entire fractional expression
FRACTIONAL_NUM = rf'(\d{{1,4}}(?:\s+\d+/\d+)?|\d{{1,4}}\s+and\s+(?:a\s+)?(?:half|quarter)|{NUM})'

# Date components
DATE_SEP = r'[./\s]+'  # Date separator: space, period, or slash
DAY = r'(\d{1,2})'     # Day: 1-2 digits
MONTH = r'([A-Za-z]+|\d{1,2})'  # Month: name or numeric
YEAR = r'(\d{4})'      # Year: 4 digits

# Full date pattern: DD sep Month sep YYYY (3 capture groups)
DATE_PATTERN = rf'{DAY}{DATE_SEP}{MONTH}{DATE_SEP}{YEAR}'

# Special day names with optional "Day" suffix
SPECIAL_DAYS = r'(Christmas(?:\s+Day)?|Midsummer(?:\s+Day)?|Lady\s+Day|Michaelmas(?:\s+Day)?)'

# Date or Special Day pattern - alternative matching
DATE_OR_SPECIAL = rf'(?:{DATE_PATTERN}|{SPECIAL_DAYS}\s+{YEAR})'

# ============================================================================
# FLEXIBLE PHRASE COMPONENTS
# ============================================================================

# Optional small words that can appear in various positions
OPT_THE = r'(?:the\s+)?'
OPT_ON = r'(?:on\s+)?'
OPT_AND = r'(?:and\s+)?'
OPT_A = r'(?:a\s+)?'
OPT_INCLUDING = r'(?:(?:and\s+)?including\s+)?'
OPT_INCLUSIVE = r'(?:\s*inclusive)?'

# Combined optional prefix: "on", "the", "and including"
OPT_PREFIX = rf'{OPT_ON}{OPT_INCLUDING}{OPT_THE}'

# Start keywords (unified): from, commencing, beginning, starting
# Also handles "on and from", "commencing from", etc.
START_KW = r'(?:from|commencing|beginning|starting)'
START_PHRASE = rf'(?:{START_KW}(?:\s+(?:on|from))?\s*{OPT_INCLUDING}|on\s+and\s+from\s+)'

# End keywords (unified): to, until, up to, ending, expiring, terminating
# Also handles compound forms like "and ending", "and expiring"
END_KW = r'(?:to|until|up\s+to|ending|expiring|terminating)'
END_PHRASE = rf'(?:{OPT_AND}{END_KW}\s*{OPT_PREFIX})'

# ============================================================================
# MODIFIER PATTERNS (less/plus days/months)
# ============================================================================

# Less days: "less 3 days", "(less 3 days)", "less the last 7 days"
LESS_DAYS = rf'(?:\s*\(?less\s+{OPT_THE}(?:last\s+)?{NUM_CAP}\s+days?\)?)?'

# Plus/and days: "plus 7 days", "and 10 days"
PLUS_DAYS = rf'(?:\s+(?:plus|and)\s+{NUM_CAP}\s+days?)?'

# Less months: "less 9 months"
LESS_MONTHS = rf'(?:\s+less\s+{NUM_CAP}\s+months?)?'

# And months: "and 6 months"
AND_MONTHS = rf'(?:\s+(?:and\s+)?{NUM_CAP}\s+months?)?'

# ============================================================================
# TERM PREFIX PATTERNS
# ============================================================================

# Optional "a term of" prefix
TERM_PREFIX = r'(?:a\s+term\s+(?:of\s+)?)?'

# Optional "for [a] term [of]" phrase
FOR_TERM = r'(?:for\s+(?:a\s+)?term\s+(?:of\s+)?)?'

# Years word with optional 's'
YEARS_WORD = r'years?'


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
    for fmt in ["%d %B %Y", "%d %b %Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Try numeric month (e.g., 29.9.1909)
    try:
        return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
    except ValueError:
        return None


def parse_month_year_date(month: str, year: str) -> Optional[datetime]:
    """
    Parse month and year into a datetime object, defaulting to the 1st day of the month.

    Args:
        month: Month name or number
        year: Year

    Returns:
        datetime object or None if parsing fails
    """
    return parse_date("1", month, year)


def parse_dol_date(dol: str) -> Optional[datetime]:
    """
    Parse a date of lease (dol) string into a datetime object.

    Handles formats like "16-10-1866" (DD-MM-YYYY), DD/MM/YYYY, DD.MM.YYYY.

    Args:
        dol: The date of lease string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if not dol:
        return None

    dol = dol.strip()
    for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"]:
        try:
            return datetime.strptime(dol, fmt)
        except ValueError:
            continue
    return None


def parse_word_number(word: str) -> Optional[int]:
    """
    Convert word numbers to integers.

    Args:
        word: Number as word (e.g., 'one', 'two') or digit string (possibly with ~ or commas)

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

    # Check if it's a digit string (possibly with ~, commas, or other chars)
    digits = re.sub(r'[^\d]', '', word)
    if digits:
        return int(digits)

    # Check word map
    return word_to_num.get(word_lower)


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

    # Handle "X and [a] half/quarter"
    match = re.match(r'^(\d+)\s+and\s+(?:a\s+)?(half|quarter)$', years_str, re.IGNORECASE)
    if match:
        base = int(match.group(1))
        fraction = match.group(2).lower()
        return base + (0.5 if fraction == 'half' else 0.25)

    # Handle "X Y/Z" format (e.g., "97 3/4")
    match = re.match(r'^(\d+)\s+(\d+)/(\d+)$', years_str)
    if match:
        base, num, denom = int(match.group(1)), int(match.group(2)), int(match.group(3))
        if denom != 0:
            return base + (num / denom)

    # Handle plain numbers (including word numbers)
    num = parse_word_number(years_str)
    return float(num) if num is not None else None


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

    try:
        year_int = int(year)
    except ValueError:
        return None

    # Normalize and lookup - handles both "Christmas" and "Christmas Day"
    day_name_lower = day_name.lower().strip().replace(' day', '')
    special_days = {
        'christmas': (12, 25),
        'midsummer': (6, 24),  # Traditional Midsummer Day in England
        'lady': (3, 25),      # Lady Day - Feast of the Annunciation
        'michaelmas': (9, 29), # Feast of St. Michael
    }

    if day_name_lower in special_days:
        month, day = special_days[day_name_lower]
        return datetime(year_int, month, day)

    return None


def _parse_date_or_special(groups: tuple, start_idx: int = 0) -> Optional[datetime]:
    """
    Parse either a regular date or a special day from match groups.

    Args:
        groups: The match groups from a regex match
        start_idx: Starting index in groups for the date/special day

    Returns:
        datetime object or None if parsing fails
    """
    # Try regular date first (3 groups: day, month, year)
    day, month, year = groups[start_idx:start_idx + 3]
    if day and month and year:
        return parse_date(day, month, year)

    # Try special day (2 groups: special_name, year)
    special_day = groups[start_idx + 3] if len(groups) > start_idx + 3 else None
    special_year = groups[start_idx + 4] if len(groups) > start_idx + 4 else None
    if special_day and special_year:
        return resolve_special_day(special_day, special_year)

    return None


def _calculate_tenure_years(start_date: datetime, expiry_date: datetime) -> int:
    """
    Calculate tenure years between two dates, rounding up when a few days short.

    If the difference is within 30 days of a full year boundary, round up.
    For example: May 3 2022 to May 2 2047 = 25 years (not 24).

    Args:
        start_date: The lease start date
        expiry_date: The lease expiry date

    Returns:
        Integer tenure in years, rounded up when close to year boundary
    """
    delta = relativedelta(expiry_date, start_date)
    years = delta.years

    # If there are remaining months (11+) and days that bring us close to another year,
    # or if we're just a few days short of the next year, round up
    if delta.months == 11 and delta.days >= 1:
        # 11 months and some days -> round up
        years += 1
    elif delta.months == 0 and delta.days < 0:
        # This shouldn't happen with relativedelta but handle edge case
        pass
    elif delta.months >= 6:
        # More than half a year remaining, could round up but be conservative
        # Only round up if very close (11 months+)
        pass

    # Also check: if adding 30 days to expiry would cross a year boundary from start
    # This handles cases like "May 3 to May 2" (one day short)
    adjusted_expiry = expiry_date + timedelta(days=30)
    adjusted_delta = relativedelta(adjusted_expiry, start_date)
    if adjusted_delta.years > years:
        years = adjusted_delta.years

    return years


def _build_result(start_date: datetime, expiry_date: datetime, tenure_years) -> Dict[str, Any]:
    """Build the standard result dictionary."""
    return {
        'start_date': start_date,
        'expiry_date': expiry_date,
        'tenure_years': tenure_years,
        'source': 'regex'
    }


def _calculate_expiry(start_date: datetime, years: float, less_days: int = 0,
                      plus_days: int = 0, less_months: int = 0, plus_months: int = 0) -> datetime:
    """Calculate expiry date from start date and tenure adjustments."""
    full_years = int(years)
    fractional_months = int(round((years - full_years) * 12))
    expiry = start_date + relativedelta(years=full_years, months=fractional_months + plus_months)
    expiry = expiry - timedelta(days=less_days) + timedelta(days=plus_days)
    expiry = expiry - relativedelta(months=less_months)
    return expiry


def parse_lease_term(term_str: str, dol: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
    - "999 years from the date of the lease" (start date from dol parameter)
    - "999 years" (start date from dol parameter)
    - "a term of years expiring on 23 June 2237" (start date from dol parameter)

    Args:
        term_str: The lease term string to parse
        dol: Optional date of lease string (e.g., "16-10-1866") used when the term
             references "date of the lease" or has no explicit start date

    Returns:
        Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
        or None if parsing fails
    """
    if not term_str:
        return None

    term_str = normalise_term_str(term_str)

    # ========================================================================
    # PATTERN 1: Years with both start AND end dates explicitly stated
    # ========================================================================
    # Examples:
    #   "10 years from and including 25 August 2020 to and including 24 August 2030"
    #   "215 years beginning on and including 24 June 1986 and ending on and including 23 June 2201"
    #   "189 years commencing on and including 01 September 1995 and expiring on and including 31 August 2184"
    #   "125 years beginning on 1 January 2013 inclusive and ending on 31 December 2138 inclusive"
    #   "22 years commencing on and including 8 November 2023 and ending on 7 November 2045"
    pattern_years_start_end = re.compile(
        rf'{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
        rf'{START_PHRASE}{OPT_THE}{DATE_PATTERN}{OPT_INCLUSIVE}\s*'
        rf'{END_PHRASE}{DATE_PATTERN}{OPT_INCLUSIVE}',
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
    # PATTERN 2: Date range without explicit years (tenure calculated)
    # ========================================================================
    # Examples:
    #   "From and including 24 June 2020 to and including 23 June 2025"
    #   "Beginning on and including 1 April 1982 and ending on and including 31 March 2197"
    #   "commencing on 28 July 2016 and expiring on 27 July 2115"
    #   "5 June 2002 until 31 December 3001"
    #   "18 December 1987 expiring on 17 December 2176"

    # Pattern 2a: With start keyword (from/beginning/commencing/starting)
    pattern_date_range = re.compile(
        rf'(?:{TERM_PREFIX})?{START_PHRASE}{OPT_THE}{DATE_PATTERN}\s*[,]?\s*'
        rf'{END_PHRASE}{DATE_PATTERN}{OPT_INCLUSIVE}',
        re.IGNORECASE
    )

    match = pattern_date_range.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = _calculate_tenure_years(start_date, expiry_date)
            return _build_result(start_date, expiry_date, tenure_years)

    # Pattern 2b: "DD Month YYYY to/until/expiring DD Month YYYY" (no start keyword)
    pattern_date_to_date = re.compile(
        rf'^{DATE_PATTERN}\s+(?:to|until|expiring\s+{OPT_ON}{OPT_INCLUDING})\s*{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_date_to_date.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = _calculate_tenure_years(start_date, expiry_date)
            return _build_result(start_date, expiry_date, tenure_years)

    # Pattern 2c: "From DD Month YYYY for a term [of years] expiring on DD Month YYYY"
    pattern_for_term_expiring = re.compile(
        rf'from\s+{OPT_INCLUDING}{DATE_PATTERN}\s+'
        rf'{FOR_TERM}{YEARS_WORD}?\s*expiring\s+{OPT_ON}{OPT_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_for_term_expiring.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        expiry_date = parse_date(match.group(4), match.group(5), match.group(6))
        if start_date and expiry_date:
            tenure_years = _calculate_tenure_years(start_date, expiry_date)
            return _build_result(start_date, expiry_date, tenure_years)


    # ========================================================================
    # PATTERN 3: Years with modifiers (less/plus days/months, fractional) + start date
    # ========================================================================
    # Consolidated pattern handling: fractional years, less/plus days, less months
    # Examples:
    #   "97 3/4 years from 25 March 1866"
    #   "65 and half years from 25 March 1904"
    #   "52 and a quarter years less 10 days from 25 March 1906"
    #   "99 years less 10 days from Midsummer Day 1852"
    #   "67 years (less 3 days) from Midsummer Day 1881"
    #   "215 years (less 3 days) from and including 24 June 1986"
    #   "500 years less 9 months from 29 September 1585"
    #   "999 Years plus 7 days from 01 November 2004"
    #   "999 years and 10 days commencing on and including 10/5/2024"
    #   "250 years less 20 days beginning on 18 October 2016"
    #   "From and including 19 September 1988 for the term of 125 years less the last 5 days"

    # Pattern 3a: Years with optional less/plus days modifier and date/special day
    pattern_years_with_modifiers = re.compile(
        rf'^{TERM_PREFIX}{FRACTIONAL_NUM}\s*{YEARS_WORD}'
        rf'(?:{LESS_DAYS}|{PLUS_DAYS}|{LESS_MONTHS})?'
        rf'\s+{START_PHRASE}{OPT_THE}'
        rf'(?:{DATE_PATTERN}|{SPECIAL_DAYS}\s+{YEAR})',
        re.IGNORECASE
    )

    match = pattern_years_with_modifiers.search(term_str)
    if match:
        years_str = match.group(1)
        years_float = parse_fractional_years(years_str)

        # Try to extract modifiers - groups vary based on which modifier matched
        less_days, plus_days, less_months = 0, 0, 0
        if match.group(2):
            less_days = parse_word_number(match.group(2)) or 0
        if match.group(3):
            plus_days = parse_word_number(match.group(3)) or 0
        if match.group(4):
            less_months = parse_word_number(match.group(4)) or 0

        # Check for regular date (groups 5,6,7) or special day (groups 8,9)
        if match.group(5):  # Regular date
            start_date = parse_date(match.group(5), match.group(6), match.group(7))
        else:  # Special day name
            start_date = resolve_special_day(match.group(8), match.group(9))

        if years_float and start_date:
            expiry_date = _calculate_expiry(start_date, years_float,
                                            less_days=less_days,
                                            plus_days=plus_days,
                                            less_months=less_months)
            return _build_result(start_date, expiry_date, years_float)

    # Pattern 3b: "From ... for [the] term [of] X years [less [the] [last] N days]"
    # Example: "From and including 19 September 1988 for the term of 125 years less the last 5 days"
    pattern_from_for_term = re.compile(
        rf'from\s+{OPT_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+(?:the\s+)?term\s+(?:of\s+)?{NUM_CAP}\s*{YEARS_WORD}'
        rf'(?:\s+less\s+(?:the\s+)?(?:last\s+)?{NUM_CAP}\s+days?)?',
        re.IGNORECASE
    )

    match = pattern_from_for_term.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        less_days = parse_word_number(match.group(5)) if match.group(5) else 0
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years, less_days=less_days)
            return _build_result(start_date, expiry_date, years)

    # Pattern 3c: Years with "and X months" modifier
    # Examples: "31 years and 6 months from 28 March 2024", "20 years and 3 months from and including 9 September 2015"
    #           "980 years 6 months from 25 March 1923" (without "and")
    pattern_years_and_months = re.compile(
        rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s*'
        rf'(?:and\s+)?{NUM_CAP}\s+months?\s*'
        rf'{START_PHRASE}{OPT_THE}{DATE_PATTERN}'
        rf'{LESS_DAYS}',
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
    # PATTERN 4: Simple years + start date (no modifiers)
    # ========================================================================
    # Consolidated patterns for: X years from/commencing/beginning DATE
    # Examples:
    #   "99 years from 24 June 1862"
    #   "999 years from the 22 December 1953"
    #   "20 years from 28/06/1996"
    #   "99 years on and from 1 June 2016"
    #   "215 years beginning on and including 24 June 1988"
    #   "Ten years beginning on and including 6 December 2016" (word number)
    #   "125 years from and including the 01 March 2023"
    #   "99 years from Christmas Day 1900" (special day)
    #   "From and including 90 years from 2 December 2024" (weird format)

    # Pattern 4a: Standard "X years from/commencing/beginning DATE/SPECIAL_DAY"
    pattern_years_from_date = re.compile(
        rf'^(?:from\s+{OPT_INCLUDING})?{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
        rf'{START_PHRASE}{OPT_THE}'
        rf'(?:{DATE_PATTERN}|{SPECIAL_DAYS}\s+{YEAR})',
        re.IGNORECASE
    )

    match = pattern_years_from_date.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        # Check for regular date (groups 2,3,4) or special day (groups 5,6)
        if match.group(2):
            start_date = parse_date(match.group(2), match.group(3), match.group(4))
        else:
            start_date = resolve_special_day(match.group(5), match.group(6))
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4b: "[commencing|beginning] on DATE for [a term of] X years"
    # Example: "commencing on 10 may 2013 for a term of 125 years"
    pattern_commencing_for_term = re.compile(
        rf'(?:commencing|beginning|starting)\s+{OPT_ON}{OPT_INCLUDING}{DATE_PATTERN}\s+'
        rf'{FOR_TERM}{NUM_CAP}\s*{YEARS_WORD}',
        re.IGNORECASE
    )

    match = pattern_commencing_for_term.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4c: "from [and including] DATE for [a term of] X years"
    # Example: "from and including 1 October 2002 for 20 years", "From 25 May 1988 for a term of 212 years"
    pattern_from_for_years = re.compile(
        rf'from\s+{OPT_INCLUDING}{DATE_PATTERN}\s+'
        rf'for\s+(?:a\s+term\s+(?:of\s+)?)?{NUM_CAP}\s*{YEARS_WORD}',
        re.IGNORECASE
    )

    match = pattern_from_for_years.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4d: "X years expiring/to [and including] DATE" (expiry-based, calculate start)
    # Examples: "147 years expiring on 23 June 2161", "15 years to and including 9 December 2039"
    pattern_years_expiring = re.compile(
        rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
        rf'(?:expiring|to)\s+{OPT_ON}{OPT_INCLUDING}{DATE_PATTERN}',
        re.IGNORECASE
    )

    match = pattern_years_expiring.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        expiry_date = parse_date(match.group(2), match.group(3), match.group(4))
        if years and expiry_date:
            start_date = expiry_date - relativedelta(years=years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4e: "starts/commencing DATE and expiring X years thereafter"
    # Example: "Commences on 28 July 2024 and expires 50 years thereafter"
    pattern_date_years_thereafter = re.compile(
        rf'{START_PHRASE}{OPT_THE}{DATE_PATTERN}\s+'
        rf'and\s+(?:expiring|expiry)\s+{NUM_CAP}\s*{YEARS_WORD}\s+thereafter',
        re.IGNORECASE
    )

    match = pattern_date_years_thereafter.search(term_str)
    if match:
        start_date = parse_date(match.group(1), match.group(2), match.group(3))
        years = parse_word_number(match.group(4))
        if start_date and years:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # Pattern 4f: "X years from [and including] Month YYYY" (no day, defaults to 1st)
    # Example: "999 years from and including December 2023", "125 years from January 2020"
    pattern_years_from_month_year = re.compile(
        rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
        rf'(?:from|commencing|beginning|starting)(?:\s+(?:on|from))?\s*(?:and\s+including\s+)?'
        rf'([A-Za-z]+)\s+(\d{{4}})(?:\s*$|\s)',
        re.IGNORECASE
    )

    match = pattern_years_from_month_year.search(term_str)
    if match:
        years = parse_word_number(match.group(1))
        start_date = parse_month_year_date(match.group(2), match.group(3))
        if years and start_date:
            expiry_date = _calculate_expiry(start_date, years)
            return _build_result(start_date, expiry_date, years)

    # ========================================================================
    # PATTERN 5: Fallback patterns (missing keywords)
    # ========================================================================
    # Examples:
    #   "999 years 25 March 1896" (missing 'from')
    #   "999 from 27 April 2006" (missing "years")

    # Pattern 5a: "X years DD Month YYYY" (missing 'from')
    pattern_years_date_no_from = re.compile(
        rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+{DATE_PATTERN}',
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
    pattern_num_from_date = re.compile(
        rf'^(\d{{1,4}})\s+from\s+{OPT_THE}{OPT_INCLUDING}{DATE_PATTERN}',
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
    # PATTERN 6: Date of Lease (dol) patterns - start date from dol field
    # ========================================================================
    # Parse dol once if provided
    dol_date = parse_dol_date(dol) if dol else None

    if dol_date:
        # Pattern 6a: "X years from [the] date [of] [the] lease"
        # Examples: "999 years from the date of the lease", "125 years from date of lease",
        #           "150 years commencing on the date of the lease"
        pattern_years_from_dol = re.compile(
            rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
            rf'(?:{START_KW}(?:\s+on)?)\s+{OPT_THE}date\s+(?:of\s+)?{OPT_THE}lease',
            re.IGNORECASE
        )

        match = pattern_years_from_dol.search(term_str)
        if match:
            years = parse_word_number(match.group(1))
            if years:
                expiry_date = _calculate_expiry(dol_date, years)
                return _build_result(dol_date, expiry_date, years)

        # Pattern 6b: "[a] term [of [years]] expiring/ending on DD Month YYYY"
        # or "[a] number of years ending on DD Month YYYY"
        # (no tenure specified, calculate from dol)
        # Examples: "a term of years expiring on 23 June 2237",
        #           "A number of years ending on 12 November 2179",
        #           "a term expiring on 31 August 2088",
        #           "term expiring on 15 March 2200"
        pattern_term_expiring = re.compile(
            rf'^(?:a\s+)?(?:term|number)(?:\s+of)?(?:\s+years?)?\s+'
            rf'(?:expiring|ending)\s+{OPT_ON}{OPT_INCLUDING}{DATE_PATTERN}',
            re.IGNORECASE
        )

        match = pattern_term_expiring.search(term_str)
        if match:
            expiry_date = parse_date(match.group(1), match.group(2), match.group(3))
            if expiry_date:
                tenure_years = _calculate_tenure_years(dol_date, expiry_date)
                return _build_result(dol_date, expiry_date, tenure_years)

        # Pattern 6c: "expiring on DD Month YYYY" (just expiry, no term prefix)
        pattern_expiring_only = re.compile(
            rf'^(?:expiring|ending)\s+{OPT_ON}{OPT_INCLUDING}{DATE_PATTERN}$',
            re.IGNORECASE
        )

        match = pattern_expiring_only.search(term_str)
        if match:
            expiry_date = parse_date(match.group(1), match.group(2), match.group(3))
            if expiry_date:
                tenure_years = _calculate_tenure_years(dol_date, expiry_date)
                return _build_result(dol_date, expiry_date, tenure_years)

        # Pattern 6d: "X years [less N days]" (just tenure, optional modifier, start from dol)
        # Example: "999 years less 6 days", "999 years"
        pattern_years_only = re.compile(
            rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}'
            rf'(?:\s+less\s+{NUM_CAP}\s+days?)?$',
            re.IGNORECASE
        )

        match = pattern_years_only.search(term_str)
        if match:
            years = parse_word_number(match.group(1))
            # Ignore less days per requirement
            if years:
                expiry_date = _calculate_expiry(dol_date, years)
                return _build_result(dol_date, expiry_date, years)

        # Pattern 6e: "X years from/commencing/beginning [and including]" (incomplete, uses dol)
        # Examples: "125 years from", "125 years from and including", "200 years commencing"
        pattern_years_from_incomplete = re.compile(
            rf'^{TERM_PREFIX}{NUM_CAP}\s*{YEARS_WORD}\s+'
            rf'(?:from|commencing|beginning|starting)(?:\s+(?:on|from))?(?:\s+and\s+including)?$',
            re.IGNORECASE
        )

        match = pattern_years_from_incomplete.search(term_str)
        if match:
            years = parse_word_number(match.group(1))
            if years:
                expiry_date = _calculate_expiry(dol_date, years)
                return _build_result(dol_date, expiry_date, years)

    # ========================================================================
    # FALLBACK: Remove parenthetical text and retry
    # ========================================================================
    # If all patterns failed and there's text in parentheses, remove it and retry
    # Example: "99 years (renewable) from 24 June 1862" -> "99 years from 24 June 1862"
    term_without_parens = re.sub(r'\s*\([^)]*\)', '', term_str).strip()
    if term_without_parens != term_str:
        return parse_lease_term(term_without_parens, dol=dol)

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
    term_str = term_str.replace(" midnight on", "")
    term_str = term_str.replace(" midnight", "")
    term_str = term_str.replace("and and", "and")
    term_str = term_str.replace("Nine hundred and ninety nine", "999")
    term_str = term_str.replace("¼", "")
    term_str = term_str.replace("½", "")
    term_str = term_str.replace("¾", "")

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
    term_str = re.sub(r'\bform\b', 'from', term_str, flags=re.IGNORECASE)  # "form" -> "from"
    term_str = re.sub(r'\bJanuaryu\b', 'January', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bJnuary\b', 'January', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bFeburary\b', 'February', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bFebuary\b', 'February', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bSeptmber\b', 'September', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bNovmber\b', 'November', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\bDecmber\b', 'December', term_str, flags=re.IGNORECASE)

    # Fix malformed phrases
    term_str = re.sub(r'\band\s+to\s+and\s+including\b', 'to and including', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\band\s+including\s+to\s+and\s+including\b', 'to and including', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\btherein\s+mentioned\b', 'the lease', term_str, flags=re.IGNORECASE)  # "date as therein mentioned" -> "date as the lease"
    term_str = re.sub(r'\bas\s+the\s+lease\b', 'of the lease', term_str, flags=re.IGNORECASE)  # "date as the lease" -> "date of the lease"

    # Fix missing space between "from" and date (e.g., "from1 January" -> "from 1 January")
    term_str = re.sub(r'\bfrom(\d)', r'from \1', term_str, flags=re.IGNORECASE)

    # Fix "including/from" -> "including" (typo with slash)
    term_str = re.sub(r'\bincluding/from\b', 'including', term_str, flags=re.IGNORECASE)

    # Remove trailing "hereof" and similar
    term_str = re.sub(r'\s+hereof\s*$', '', term_str, flags=re.IGNORECASE)
    term_str = re.sub(r'\s+thereof\s*$', '', term_str, flags=re.IGNORECASE)

    return term_str

