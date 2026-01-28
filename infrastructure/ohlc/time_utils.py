from datetime import datetime, timezone, timedelta
from typing import Optional


# Indian Standard Time
IST = timezone(timedelta(hours=5, minutes=30))


class TimeUtil:
    """Ultra-fast time conversion utilities."""

    @staticmethod
    def timestamp_ms_to_minute_int(timestamp_ms: int) -> int:
        """
        Convert epoch milliseconds to HHMMSS minute code.
        
        Examples:
            9:15:00 → 91500
            9:16:00 → 91600
            15:30:00 → 153000
        
        Args:
            timestamp_ms: Unix epoch time in milliseconds
        
        Returns:
            Integer in HHMM00 format (e.g., 91500)
        """
        if timestamp_ms <= 0:
            return 0
        
        dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=IST)
        return dt.hour * 10000 + dt.minute * 100
    
    @staticmethod
    def minute_int_to_timestamp_ms(minute_int: int, date_ms: Optional[int] = None) -> int:
        """
        Convert HHMMSS minute code back to epoch milliseconds.
        
        Args:
            minute_int: Integer in HHMM00 format (e.g., 91500)
            date_ms: Optional date context (defaults to today)
        
        Returns:
            Unix epoch time in milliseconds
        """
        if date_ms:
            dt = datetime.fromtimestamp(date_ms / 1000.0, tz=IST)
        else:
            dt = datetime.now(tz=IST)
        
        hour = minute_int // 10000
        minute = (minute_int % 10000) // 100
        
        dt = dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)
    
    @staticmethod
    def get_minute_start_ms(timestamp_ms: int, interval_ms: int = 60_000) -> int:
        """
        Get the start of the minute bucket for a given timestamp.
        
        Args:
            timestamp_ms: Unix epoch time in milliseconds
            interval_ms: Bucket size (default 60,000 = 1 minute)
        
        Returns:
            Bucket start time in milliseconds (multiple of interval)
        """
        return timestamp_ms - (timestamp_ms % interval_ms)
    
    @staticmethod
    def is_market_hours(timestamp_ms: int) -> bool:
        """
        Check if timestamp falls within market hours (9:15 - 15:30 IST).
        
        Args:
            timestamp_ms: Unix epoch time in milliseconds
        
        Returns:
            True if within market hours
        """
        minute_int = TimeUtil.timestamp_ms_to_minute_int(timestamp_ms)
        return 91500 <= minute_int <= 153000
    
    @staticmethod
    def format_minute_int(minute_int: int) -> str:
        """
        Format minute_int as readable time string.
        
        Args:
            minute_int: Integer in HHMM00 format (e.g., 91500)

        Returns:
            Formatted string like "09:15:00"
        """
        hour = minute_int // 10000
        minute = (minute_int % 10000) // 100
        return f"{hour:02d}:{minute:02d}:00"
