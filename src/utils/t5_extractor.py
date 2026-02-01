"""
T5-based extractor for lease term parsing.

Uses a fine-tuned T5 model to extract lease start date, end date, and tenure
from various string formats. This serves as a fallback for records that
the regex extractor couldn't handle.
"""

import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional, Dict, Any

import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration


class T5LeaseExtractor:
    """T5-based lease term extractor with lazy model loading."""

    def __init__(self, model_path: str = "./t5_model/trained_t5"):
        """
        Initialize the T5 extractor.

        Args:
            model_path: Path to the trained T5 model directory
        """
        self.model_path = model_path
        self._tokenizer = None
        self._model = None
        self._max_length = 64

    @property
    def tokenizer(self):
        """Lazy load the tokenizer."""
        if self._tokenizer is None:
            self._tokenizer = T5Tokenizer.from_pretrained(self.model_path, legacy=False)
        return self._tokenizer

    @property
    def model(self):
        """Lazy load the model."""
        if self._model is None:
            self._model = T5ForConditionalGeneration.from_pretrained(self.model_path)
            self._model.eval()
        return self._model

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse a date string into a datetime object.

        Handles formats like:
        - DD/MM/YYYY
        - DD.MM.YYYY
        - DD-MM-YYYY
        - "Not specified"
        - Special day names like "Christmas Day 1900"

        Args:
            date_str: The date string to parse

        Returns:
            datetime object or None if parsing fails
        """
        if not date_str or date_str.lower() in ('not specified', 'residential', ''):
            return None

        date_str = date_str.strip()

        # Try standard date formats
        for fmt in ["%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # Handle special day names (e.g., "Christmas Day 1900")
        special_days = {
            'christmas': (12, 25),
            'midsummer': (6, 24),
            'lady day': (3, 25),
            'michaelmas': (9, 29),
        }

        date_str_lower = date_str.lower()
        for day_name, (month, day) in special_days.items():
            if day_name in date_str_lower:
                # Extract year from string
                year_match = re.search(r'\d{4}', date_str)
                if year_match:
                    return datetime(int(year_match.group()), month, day)

        return None

    def _parse_tenure(self, tenure_str: str) -> Optional[int]:
        """
        Parse a tenure string into years.

        Handles formats like:
        - "99 years"
        - "999 years"
        - "25 years less 3 days"
        - "Not specified"

        Args:
            tenure_str: The tenure string to parse

        Returns:
            Integer years or None if parsing fails
        """
        if not tenure_str or tenure_str.lower() in ('not specified', 'residential', ''):
            return None

        # Extract the primary year number
        match = re.search(r'(\d+)\s*years?', tenure_str, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return None

    def _parse_t5_output(self, output: str) -> Dict[str, Any]:
        """
        Parse the T5 model output string into structured data.

        The T5 model outputs concatenated values without clear delimiters:
        - start_date (DD/MM/YYYY or text or "Not specified")
        - end_date (DD/MM/YYYY or "Not specified")
        - tenure (e.g., "99 years" or "Not specified")

        Args:
            output: The raw T5 model output string

        Returns:
            Dictionary with parsed components
        """
        if not output:
            return {'start_date': None, 'expiry_date': None, 'tenure_years': None}

        output = output.strip()

        # Try to find date patterns (DD/MM/YYYY)
        date_pattern = r'\d{2}/\d{2}/\d{4}'
        dates = re.findall(date_pattern, output)

        start_date = None
        expiry_date = None
        tenure_years = None

        if len(dates) >= 1:
            start_date = self._parse_date(dates[0])
        if len(dates) >= 2:
            expiry_date = self._parse_date(dates[1])

        # Extract tenure from the remaining text
        # Remove dates from the output to find tenure
        remaining = re.sub(date_pattern, '', output)
        remaining = remaining.replace('Not specified', '').strip()

        if remaining:
            tenure_years = self._parse_tenure(remaining)

        # If we only have "Not specified" entries, the output might be just a tenure
        if not dates and not start_date and not expiry_date:
            # Try parsing the whole output for special cases
            tenure_years = self._parse_tenure(output)

            # Check for special day + year patterns
            special_match = re.search(r'(Christmas|Midsummer|Lady|Michaelmas)(?:\s+Day)?\s+(\d{4})', output, re.IGNORECASE)
            if special_match:
                day_name = special_match.group(1).lower()
                year = int(special_match.group(2))
                special_days = {
                    'christmas': (12, 25),
                    'midsummer': (6, 24),
                    'lady': (3, 25),
                    'michaelmas': (9, 29),
                }
                if day_name in special_days:
                    month, day = special_days[day_name]
                    start_date = datetime(year, month, day)

        # If we have start_date and tenure but no expiry, calculate expiry
        if start_date and tenure_years and not expiry_date:
            expiry_date = start_date + relativedelta(years=tenure_years)

        # If we have start and expiry but no tenure, calculate tenure
        if start_date and expiry_date and not tenure_years:
            delta = relativedelta(expiry_date, start_date)
            tenure_years = delta.years
            # Round up if close to a year boundary
            if delta.months >= 6:
                tenure_years += 1

        return {
            'start_date': start_date,
            'expiry_date': expiry_date,
            'tenure_years': tenure_years
        }

    def extract(self, term_str: str, dol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Extract lease term data from a term string using the T5 model.

        Args:
            term_str: The lease term string to parse
            dol: Optional date of lease string (for patterns referencing "date of lease")

        Returns:
            Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
            or None if extraction fails completely
        """
        if not term_str or not term_str.strip():
            return None

        # Prepare input for T5
        input_text = f"parse lease: {term_str}"
        input_ids = self.tokenizer(
            input_text,
            max_length=self._max_length,
            padding='max_length',
            truncation=True,
            return_tensors='pt'
        ).input_ids

        # Generate output
        with torch.no_grad():
            output_ids = self.model.generate(
                input_ids,
                max_length=self._max_length,
                num_beams=4,
                early_stopping=True
            )

        # Decode output
        raw_output = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

        # Parse the T5 output
        parsed = self._parse_t5_output(raw_output)

        # If start_date not found and dol is provided, use it
        if parsed['start_date'] is None and dol:
            parsed['start_date'] = self._parse_dol_date(dol)
            # Recalculate expiry if we now have start_date and tenure
            if parsed['start_date'] and parsed['tenure_years'] and not parsed['expiry_date']:
                parsed['expiry_date'] = parsed['start_date'] + relativedelta(years=parsed['tenure_years'])

        # Check if we have enough data to be valid
        has_valid_data = (
            (parsed['start_date'] is not None and parsed['expiry_date'] is not None) or
            (parsed['start_date'] is not None and parsed['tenure_years'] is not None) or
            (parsed['expiry_date'] is not None and parsed['tenure_years'] is not None)
        )

        if not has_valid_data:
            return None

        return {
            'start_date': parsed['start_date'],
            'expiry_date': parsed['expiry_date'],
            'tenure_years': parsed['tenure_years'],
            'extractor': 't5'
        }

    def _parse_dol_date(self, dol: str) -> Optional[datetime]:
        """
        Parse a date of lease (dol) string into a datetime object.

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


# Global extractor instance (lazy-loaded)
_extractor: Optional[T5LeaseExtractor] = None


def get_extractor(model_path: str = "./t5_model/trained_t5") -> T5LeaseExtractor:
    """
    Get the global T5 extractor instance (lazy-loaded).

    Args:
        model_path: Path to the trained T5 model directory

    Returns:
        T5LeaseExtractor instance
    """
    global _extractor
    if _extractor is None:
        _extractor = T5LeaseExtractor(model_path)
    return _extractor


def parse_lease_term_t5(term_str: str, dol: Optional[str] = None,
                        model_path: str = "./t5_model/trained_t5") -> Optional[Dict[str, Any]]:
    """
    Parse a lease term string using the T5 model.

    This is a convenience function that uses the global extractor instance.

    Args:
        term_str: The lease term string to parse
        dol: Optional date of lease string (for patterns referencing "date of lease")
        model_path: Path to the trained T5 model directory

    Returns:
        Dictionary with 'start_date', 'expiry_date', 'tenure_years', and 'source' keys,
        or None if extraction fails
    """
    extractor = get_extractor(model_path)
    return extractor.extract(term_str, dol=dol)

