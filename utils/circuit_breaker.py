"""
Circuit Breaker Pattern for Redis Fault Isolation.

Prevents cascading failures by temporarily halting operations after repeated failures.
"""
import asyncio
import time
from enum import Enum
from typing import Optional, Callable, Any
from dataclasses import dataclass
from utils.logger import setup_logger

logger = setup_logger("circuit-breaker")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing - block all requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5  # Failures before opening circuit
    success_threshold: int = 2  # Successes to close from half-open
    timeout_seconds: float = 30.0  # Time before attempting recovery
    half_open_max_calls: int = 3  # Max calls in half-open state


class CircuitBreaker:
    """
    Circuit breaker for Redis operations.

    States:
    - CLOSED: Normal operation, counts failures
    - OPEN: Too many failures, block all requests
    - HALF_OPEN: Testing recovery, allow limited requests

    Example:
        breaker = CircuitBreaker(name="redis-writer")

        async with breaker:
            await redis_operation()
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.on_state_change = on_state_change

        # State
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0

        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_successes = 0
        self.circuit_opens = 0

        logger.info(
            f"Circuit breaker '{name}' initialized | "
            f"threshold={config.failure_threshold} | "
            f"timeout={config.timeout_seconds}s"
        )

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        return self._state

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests)."""
        return self._state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self._state == CircuitState.CLOSED

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to new state."""
        if new_state == self._state:
            return

        old_state = self._state
        self._state = new_state

        # Reset counters on state change
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
        elif new_state == CircuitState.OPEN:
            self._success_count = 0
            self._half_open_calls = 0
            self.circuit_opens += 1
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._success_count = 0

        logger.warning(
            f"🔄 Circuit '{self.name}' state change: {old_state.value} → {new_state.value}"
        )

        if self.on_state_change:
            try:
                self.on_state_change(old_state, new_state)
            except Exception as e:
                logger.error(f"State change callback error: {e}")

    def _should_attempt_reset(self) -> bool:
        """Check if enough time passed to attempt recovery."""
        if self._state != CircuitState.OPEN:
            return False

        if self._last_failure_time is None:
            return True

        elapsed = time.time() - self._last_failure_time
        return elapsed >= self.config.timeout_seconds

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker.

        Args:
            func: Async function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpenError: If circuit is open
            Exception: Original exception from function
        """
        self.total_calls += 1

        # Check if circuit is open
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                logger.info(f"Circuit '{self.name}' attempting recovery (HALF_OPEN)")
                self._transition_to(CircuitState.HALF_OPEN)
            else:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN - operation blocked"
                )

        # Half-open: limit concurrent calls
        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.config.half_open_max_calls:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' HALF_OPEN - max calls exceeded"
                )
            self._half_open_calls += 1

        # Execute function
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful call."""
        self.total_successes += 1

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                logger.info(
                    f"✅ Circuit '{self.name}' recovered - closing circuit"
                )
                self._transition_to(CircuitState.CLOSED)
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self.total_failures += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Failed recovery attempt - reopen circuit
            logger.warning(
                f"❌ Circuit '{self.name}' recovery failed - reopening"
            )
            self._transition_to(CircuitState.OPEN)
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.config.failure_threshold:
                logger.error(
                    f"🚨 Circuit '{self.name}' OPENED after "
                    f"{self._failure_count} failures"
                )
                self._transition_to(CircuitState.OPEN)

    async def __aenter__(self):
        """Context manager entry."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to(CircuitState.HALF_OPEN)
            else:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN"
                )

        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.config.half_open_max_calls:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' HALF_OPEN - max calls exceeded"
                )
            self._half_open_calls += 1

        self.total_calls += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is None:
            # Success
            self._on_success()
        else:
            # Failure
            self._on_failure()
        return False  # Don't suppress exception

    def reset(self) -> None:
        """Manually reset circuit breaker."""
        logger.info(f"Circuit '{self.name}' manually reset")
        self._transition_to(CircuitState.CLOSED)

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "total_calls": self.total_calls,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "circuit_opens": self.circuit_opens,
        }


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass
