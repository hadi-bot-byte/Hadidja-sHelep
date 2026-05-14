"""
Test script for Circuit Breaker pattern
"""

import time
import sys
sys.path.append('.')

from circuit_breaker import get_circuit_breaker, CircuitBreakerState

def successful_function():
    """Function that always succeeds"""
    return "Success!"

def failing_function():
    """Function that always fails"""
    raise Exception("Service unavailable")

def flaky_function(succeed_after=3):
    """Function that fails a few times then succeeds"""
    if not hasattr(flaky_function, 'call_count'):
        flaky_function.call_count = 0
    flaky_function.call_count += 1
    
    if flaky_function.call_count < succeed_after:
        raise Exception(f"Failure #{flaky_function.call_count}")
    return f"Success after {flaky_function.call_count} attempts"

# Test 1: Normal operation (CLOSED circuit)
print("=== Test 1: Normal operation ===")
cb = get_circuit_breaker("test1", failure_threshold=3, timeout=5)
for i in range(5):
    try:
        result = cb.call(successful_function)
        print(f"Attempt {i+1}: {result}")
    except Exception as e:
        print(f"Attempt {i+1}: FAILED - {e}")
print(f"Final state: {cb.get_state()}")
print()

# Test 2: Circuit opens after failures
print("=== Test 2: Circuit opens after failures ===")
cb2 = get_circuit_breaker("test2", failure_threshold=3, timeout=10)
for i in range(5):
    try:
        result = cb2.call(failing_function)
        print(f"Attempt {i+1}: {result}")
    except Exception as e:
        print(f"Attempt {i+1}: FAILED - {e}")
    print(f"  Circuit state: {cb2.state.value}, failures: {cb2.failure_count}")
print(f"Final state: {cb2.get_state()}")
print()

# Test 3: Circuit recovery (HALF_OPEN -> CLOSED)
print("=== Test 3: Circuit recovery ===")
cb3 = get_circuit_breaker("test3", failure_threshold=2, timeout=3)

# First, cause failures to open circuit
print("Causing failures...")
for i in range(2):
    try:
        cb3.call(failing_function)
    except:
        pass
print(f"Circuit state after failures: {cb3.state.value}")

# Wait for timeout
print(f"Waiting {cb3.timeout} seconds for timeout...")
time.sleep(cb3.timeout + 1)

# Try a successful call - should be HALF_OPEN then CLOSED
print("Attempting recovery call...")
try:
    result = cb3.call(successful_function)
    print(f"Recovery succeeded: {result}")
except Exception as e:
    print(f"Recovery failed: {e}")

print(f"Final state: {cb3.get_state()}")
print()

# Test 4: Flaky service recovery
print("=== Test 4: Flaky service (fails 3 times then succeeds) ===")
# Reset call count
flaky_function.call_count = 0
cb4 = get_circuit_breaker("test4", failure_threshold=3, timeout=5)

for i in range(6):
    try:
        result = cb4.call(flaky_function, 3)  # Succeed after 3 failures
        print(f"Attempt {i+1}: {result}")
    except Exception as e:
        print(f"Attempt {i+1}: FAILED - {e}")
    print(f"  Circuit state: {cb4.state.value}, failures: {cb4.failure_count}")