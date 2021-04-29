"""Common utilities for parsing Tasty CSV files."""

import decimal
from decimal import Decimal


def ToDecimal(value: str):
    """Convert number to decimal."""
    if value == "--":
        return Decimal(0)
    try:
        return Decimal(value.replace(',', '') if value else 0)
    except decimal.InvalidOperation as exc:
        raise ValueError(f"Invalid operation: Could not parse '{value}'")
