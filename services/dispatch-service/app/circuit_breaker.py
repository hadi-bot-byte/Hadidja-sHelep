"""
Circuit Breaker Pattern Implementation
States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
"""

import time
import logging
from enum import Enum
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation - requests flow through
    OPEN = "open"          # Failing state - requests rejected immediately
    HALF_OPEN = "half_open" # Testing state - allow test request to check recovery

class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures in distributed systems.
    
    Tracks failures and opens the circuit when threshold exceeded.
    After timeout, allows test request to check if service recovered.
    """
    
    def __init__(
        self, 
        failure_threshold: int = 5, 
        timeout: int = 60,
        name: str = "default"
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before circuit opens
            timeout: Seconds to wait before attempting recovery (in seconds)
            name: Circuit breaker name for logging
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.name = name
        
        # State tracking
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.total_failures = 0
        self.total_successes = 0
        
        logger.info(f"CircuitBreaker '{name}' initialized: state=CLOSED, threshold={failure_threshold}, timeout={timeout}s")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result of func call
            
        Raises:
            Exception: If circuit is OPEN or func fails
        """
        
        # Check circuit state before executing
        if self.state == CircuitBreakerState.OPEN:
            # Check if timeout has elapsed
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.timeout:
                logger.info(f"CircuitBreaker '{self.name}': Timeout elapsed, transitioning to HALF_OPEN")
                self.state = CircuitBreakerState.HALF_OPEN
            else:
                # Circuit is still OPEN - reject request
                elapsed = time.time() - self.last_failure_time if self.last_failure_time else 0
                logger.warning(f"CircuitBreaker '{self.name}': Circuit OPEN - rejecting request (elapsed={elapsed:.1f}s, timeout={self.timeout}s)")
                raise Exception(f"Circuit breaker '{self.name}' is OPEN - service unavailable (failed at {self.last_failure_time})")
        
        # Execute the function (CLOSED or HALF_OPEN state)
        try:
            result = func(*args, **kwargs)
            
            # Success handling
            self.total_successes += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                # Success in HALF_OPEN - close the circuit
                logger.info(f"CircuitBreaker '{self.name}': Success in HALF_OPEN - closing circuit")
                self.reset()
            elif self.state == CircuitBreakerState.CLOSED:
                # Success in CLOSED - reset failure count
                self.failure_count = 0
                self.last_success_time = time.time()
                logger.debug(f"CircuitBreaker '{self.name}': Success in CLOSED state (failures={self.failure_count}/{self.failure_threshold})")
            
            return result
            
        except Exception as e:
            # Failure handling
            self.total_failures += 1
            self.record_failure()
            
            logger.error(f"CircuitBreaker '{self.name}': Function failed - {str(e)}")
            
            # Re-raise the original exception
            raise e
    
    def record_failure(self) -> None:
        """Record a failure and potentially open the circuit."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        logger.warning(f"CircuitBreaker '{self.name}': Failure recorded (failures={self.failure_count}/{self.failure_threshold})")
        
        # Check if threshold exceeded
        if self.failure_count >= self.failure_threshold:
            self.open_circuit()
    
    def open_circuit(self) -> None:
        """Open the circuit - start rejecting requests."""
        if self.state != CircuitBreakerState.OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.error(f"CircuitBreaker '{self.name}': Circuit OPENED after {self.failure_count} failures")
    
    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        logger.info(f"CircuitBreaker '{self.name}': Circuit RESET to CLOSED state")
    
    def get_state(self) -> dict:
        """Get current circuit breaker state for monitoring."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "last_failure": self.last_failure_time,
            "last_success": self.last_success_time,
            "timeout": self.timeout
        }
    
    def __str__(self) -> str:
        """String representation for logging."""
        return f"CircuitBreaker({self.name})[state={self.state.value}, failures={self.failure_count}/{self.failure_threshold}]"


# Singleton circuit breakers for different services
_circuit_breakers = {}

def get_circuit_breaker(name: str, failure_threshold: int = 5, timeout: int = 60) -> CircuitBreaker:
    """
    Get or create a circuit breaker by name.
    
    Args:
        name: Circuit breaker identifier
        failure_threshold: Number of failures before opening circuit
        timeout: Recovery timeout in seconds
        
    Returns:
        CircuitBreaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(failure_threshold, timeout, name)
    return _circuit_breakers[name]