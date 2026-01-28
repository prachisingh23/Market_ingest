"""
Centralized validation and sanitization utilities.
"""
from typing import Any, Optional
def to_int(value: Any) -> Optional[int]:
    """Convert value to int, return None on failure."""
    try:
        return int(float(value))
    except (TypeError, ValueError, AttributeError):
        return None
def to_float(value: Any) -> Optional[float]:
    """Convert value to float, return None on failure."""
    try:
        return float(value)
    except (TypeError, ValueError, AttributeError):
        return None
def to_bool(value: Any, default: bool = False) -> bool:
    """Convert value to bool with intelligent parsing."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", ""}:
            return False
    return default
# Cache for sanitized tokens to avoid repeated computation
_SANITIZED_TOKEN_CACHE: dict[str, str] = {}
_SAFE_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")
def sanitize_token(token: str) -> str:
    """
    Sanitize token for Redis key safety (O(1) with caching).
    Replaces spaces and special chars with underscores.
    Examples:
        "Nifty Bank" -> "Nifty_Bank"
        "Nifty 50" -> "Nifty_50"
    """
    if not token:
        return token
    # Check cache first
    if token in _SANITIZED_TOKEN_CACHE:
        return _SANITIZED_TOKEN_CACHE[token]
    # Fast path: if already safe, return as-is
    if all(c in _SAFE_CHARS for c in token):
        _SANITIZED_TOKEN_CACHE[token] = token
        return token
    # Sanitize: replace spaces and unsafe chars with underscore
    sanitized = "".join(c if c in _SAFE_CHARS else "_" for c in token)
    # Cache result
    _SANITIZED_TOKEN_CACHE[token] = sanitized
    return sanitized
def validate_tick_data(
    exchange: Optional[str],
    token: Optional[str],
    ltp: Optional[float],
    timestamp_ms: Optional[int]
) -> bool:
    """
    Validate required tick fields in one call.
    Returns:
        True if all required fields are valid
    """
    return bool(
        exchange and 
        token and 
        ltp is not None and 
        ltp > 0 and 
        timestamp_ms is not None and 
        timestamp_ms > 0
    )
def clear_token_cache() -> None:
    """Clear token sanitization cache (for testing)."""
    _SANITIZED_TOKEN_CACHE.clear()
