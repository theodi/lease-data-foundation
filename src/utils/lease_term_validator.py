"""
Lease term validation module.

Validates the output of regex_extractors.parse_lease_term to ensure
the extracted lease data is logically consistent and valid.
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, List, Optional


class LeaseTermValidationError:
    """Represents a validation error with code and message."""

    def __init__(self, code: str, message: str, severity: str = "error"):
        """
        Initialize a validation error.

        Args:
            code: Error code identifier
            message: Human-readable error message
            severity: 'error' or 'warning'
        """
        self.code = code
        self.message = message
        self.severity = severity

    def __repr__(self):
        return f"LeaseTermValidationError(code='{self.code}', message='{self.message}', severity='{self.severity}')"

    def __eq__(self, other):
        if not isinstance(other, LeaseTermValidationError):
            return False
        return self.code == other.code and self.message == other.message and self.severity == other.severity


class LeaseTermValidationResult:
    """Result of lease term validation."""

    def __init__(self):
        self.errors: List[LeaseTermValidationError] = []
        self.warnings: List[LeaseTermValidationError] = []

    @property
    def is_valid(self) -> bool:
        """Returns True if there are no errors (warnings are acceptable)."""
        return len(self.errors) == 0

    def add_error(self, code: str, message: str):
        """Add an error to the result."""
        self.errors.append(LeaseTermValidationError(code, message, "error"))

    def add_warning(self, code: str, message: str):
        """Add a warning to the result."""
        self.warnings.append(LeaseTermValidationError(code, message, "warning"))

    def __repr__(self):
        return f"LeaseTermValidationResult(is_valid={self.is_valid}, errors={len(self.errors)}, warnings={len(self.warnings)})"


def validate_lease_term(
    lease_data: Optional[Dict[str, Any]],
    reference_date: Optional[datetime] = None,
    tolerance_days: int = 10
) -> LeaseTermValidationResult:
    """
    Validate the output of parse_lease_term.

    Performs the following validations:
    - lease_data is not None
    - Required fields are present (start_date, expiry_date, tenure_years)
    - start_date is before expiry_date
    - start_date + tenure_years approximately equals expiry_date (within tolerance)
    - start_date is before reference_date (defaults to today)
    - tenure_years is positive
    - Dates are reasonable (not too far in the past or future)

    Args:
        lease_data: Dictionary output from parse_lease_term
        reference_date: Date to compare start_date against (defaults to today)
        tolerance_days: Number of days tolerance for tenure calculation mismatch

    Returns:
        LeaseTermValidationResult with errors and warnings
    """
    result = LeaseTermValidationResult()

    if reference_date is None:
        reference_date = datetime.now()

    # Validate lease_data is not None
    if lease_data is None:
        result.add_error("NULL_DATA", "Lease data is None - parsing may have failed")
        return result

    # Validate required fields
    required_fields = ['start_date', 'expiry_date', 'tenure_years']
    for field in required_fields:
        if field not in lease_data:
            result.add_error("MISSING_FIELD", f"Required field '{field}' is missing")

    # If required fields are missing, return early
    if not result.is_valid:
        return result

    start_date = lease_data['start_date']
    expiry_date = lease_data['expiry_date']
    tenure_years = lease_data['tenure_years']

    # Validate field types
    if not isinstance(start_date, datetime):
        result.add_error("INVALID_TYPE", f"start_date must be datetime, got {type(start_date).__name__}")
    if not isinstance(expiry_date, datetime):
        result.add_error("INVALID_TYPE", f"expiry_date must be datetime, got {type(expiry_date).__name__}")
    if not isinstance(tenure_years, (int, float)):
        result.add_error("INVALID_TYPE", f"tenure_years must be numeric, got {type(tenure_years).__name__}")

    # If types are invalid, return early
    if not result.is_valid:
        return result

    # Validate start_date is before expiry_date
    if start_date >= expiry_date:
        result.add_error(
            "INVALID_DATE_ORDER",
            f"start_date ({start_date.strftime('%Y-%m-%d')}) must be before expiry_date ({expiry_date.strftime('%Y-%m-%d')})"
        )

    # Validate tenure_years is positive
    if tenure_years <= 0:
        result.add_error("INVALID_TENURE", f"tenure_years must be positive, got {tenure_years}")

    # Validate start_date + tenure_years approximately equals expiry_date
    if tenure_years > 0:
        calculated_expiry = start_date + relativedelta(years=int(tenure_years))
        date_diff = abs((calculated_expiry - expiry_date).days)

        if date_diff > tolerance_days:
            result.add_warning(
                "TENURE_MISMATCH",
                f"start_date + tenure_years ({calculated_expiry.strftime('%Y-%m-%d')}) differs from "
                f"expiry_date ({expiry_date.strftime('%Y-%m-%d')}) by {date_diff} days"
            )

    # Validate start_date is before reference_date (lease should have started)
    if start_date > reference_date:
        result.add_warning(
            "FUTURE_START_DATE",
            f"start_date ({start_date.strftime('%Y-%m-%d')}) is in the future"
        )

    # Validate dates are reasonable (not before 1800)
    min_reasonable_date = datetime(1800, 1, 1)
    if start_date < min_reasonable_date:
        result.add_warning(
            "UNREASONABLE_START_DATE",
            f"start_date ({start_date.strftime('%Y-%m-%d')}) is before 1800, which seems unreasonable"
        )

    # Validate expiry_date is not too far in the future (more than 1000 years from start)
    max_tenure_years = 1000
    if tenure_years > max_tenure_years:
        result.add_warning(
            "EXCESSIVE_TENURE",
            f"tenure_years ({tenure_years}) exceeds {max_tenure_years} years, which seems excessive"
        )

    # Validate lease hasn't already expired (warning only)
    if expiry_date < reference_date:
        result.add_warning(
            "LEASE_EXPIRED",
            f"Lease has expired on {expiry_date.strftime('%Y-%m-%d')}"
        )

    return result


def is_lease_term_valid(
    lease_data: Optional[Dict[str, Any]],
    reference_date: Optional[datetime] = None,
    tolerance_days: int = 10
) -> bool:
    """
    Quick check if lease term data is valid.

    Args:
        lease_data: Dictionary output from parse_lease_term
        reference_date: Date to compare start_date against (defaults to today)
        tolerance_days: Number of days tolerance for tenure calculation mismatch

    Returns:
        True if valid (no errors), False otherwise
    """
    result = validate_lease_term(lease_data, reference_date, tolerance_days)
    return result.is_valid

