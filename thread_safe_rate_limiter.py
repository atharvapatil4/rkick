import unittest
import time
from collections import deque
from threading import Lock, Thread, Barrier
from typing import Dict, List

# =================================================================
#  YOUR IMPLEMENTATION GOES IN THIS CLASS
# =================================================================
"""
The Problem: A Thread-Safe Rate Limiter
In backend systems, rate limiting is crucial for preventing abuse and ensuring service stability. Your task is to implement a RateLimiter class in Python.

Requirements:

The RateLimiter is initialized with a requests_per_minute value.
It must have a single method: is_allowed(client_id: str) -> bool.

This method returns True if the client_id is within their request limit for the last 60 seconds, and False otherwise.

The implementation must be thread-safe. Multiple threads will call is_allowed concurrently for various client_ids. 
Your solution must prevent race conditions and ensure the rate limit is accurately enforced.

The solution should be memory-efficient. Imagine this class needs to handle tens of thousands of unique client_ids.
"""
class RateLimiter:
    """
    A thread-safe rate limiter that enforces a rule of a certain number
    of requests per minute for each client.
    """
    class ClientMetadata:
        def __init__(self):
            self.lock = Lock()
            self.q = deque()

    def __init__(self, requests_per_minute: int):
        """
        Initializes the RateLimiter.

        Args:
            requests_per_minute: The number of allowed requests per minute per client.
        """
        # --- YOUR CODE HERE ---
        ClientData = Dict[str, self.ClientMetadata]
        # Choose a data structure to store request timestamps for each client.
        # Consider thread safety for this shared data structure.
        # we can assume the timestamps are increasing
        self.requests_per_minute = requests_per_minute
        # client_id -> {lock, deque of timestamps}
        self.clients: Dict[str, ClientData] = {}
        # A lock to protect the self.clients dictionary itself when adding new clients
        self.clients_lock = Lock()

    def is_older_than_60_seconds(self, timestamp_a, timestamp_b):
        return timestamp_b - timestamp_a > 60

    def is_allowed(self, client_id: str) -> bool:
        """
        Checks if a request from a given client is allowed.

        This method must be thread-safe.

        Args:
            client_id: The unique identifier for the client.

        Returns:
            True if the request is within the limit, False otherwise.
        """
        current_time = time.time()

        # --- YOUR CODE HERE ---
        # 1. Get the request timestamps for the given client_id.
        #    - This part needs to be thread-safe. How do you handle a new client?
        timestamp = time.time()
        if client_id not in self.clients:
            with self.clients_lock:
                meta = self.ClientMetadata()
                meta.q.append(timestamp)
                self.clients[client_id] = meta
                return True
        with self.clients[client_id].lock:
            q: deque = self.clients[client_id].q
            # peek first
            while self.is_older_than_60_seconds(q[0], timestamp):
                q.popleft()
            if len(q) < self.requests_per_minute:
                q.append(timestamp)
                return True
        return False
            
        # 3. Check if the number of remaining timestamps is less than the limit.
        # 4. If it is, add the new timestamp and return True.
        # 5. Otherwise, return False.
        
        # This is a placeholder. Replace it with your actual logic.


# =================================================================
#  TEST SUITE (MODIFIED TO MATCH YOUR IMPLEMENTATION)
# =================================================================

class TestRateLimiter(unittest.TestCase):

    def test_single_client_within_limit(self):
        """Tests that a client can make requests up to the limit."""
        limiter = RateLimiter(requests_per_minute=5)
        client_id = "client-1"
        for _ in range(5):
            self.assertTrue(limiter.is_allowed(client_id), "Should allow requests within limit")

    def test_single_client_exceeds_limit(self):
        """Tests that a client is blocked after exceeding the limit."""
        limiter = RateLimiter(requests_per_minute=3)
        client_id = "client-2"
        for _ in range(3):
            limiter.is_allowed(client_id)
        self.assertFalse(limiter.is_allowed(client_id), "Should block requests exceeding limit")

    def test_time_window_resets(self):
        """Tests that the limit resets after the time window passes."""
        limiter = RateLimiter(requests_per_minute=2)
        client_id = "client-3"
        
        # Use up the limit
        self.assertTrue(limiter.is_allowed(client_id))
        self.assertTrue(limiter.is_allowed(client_id))
        self.assertFalse(limiter.is_allowed(client_id))

        # To simulate the window passing, we can manually manipulate timestamps
        # This is a bit of a hack for testing, but effective.
        with limiter.clients_lock:
            if client_id in limiter.clients:
                try:
                    # Access the client's metadata object based on your implementation
                    client_meta = limiter.clients[client_id]
                    with client_meta.lock:
                        # Set the timestamp to be 70 seconds in the past
                        if client_meta.q:
                            client_meta.q[0] = time.time() - 70
                except (IndexError, ValueError, KeyError, AttributeError) as e:
                    print(f"\n[Warning] Could not run time_window_resets test due to differing internal structure. Error: {e}")


        # Now, the request should be allowed again
        self.assertTrue(limiter.is_allowed(client_id), "Should allow requests after window reset")

    def test_multiple_clients_are_independent(self):
        """Tests that limits for different clients are tracked independently."""
        limiter = RateLimiter(requests_per_minute=2)
        client_a = "client-A"
        client_b = "client-B"

        # Client A uses its limit
        self.assertTrue(limiter.is_allowed(client_a))
        self.assertTrue(limiter.is_allowed(client_a))
        self.assertFalse(limiter.is_allowed(client_a))

        # Client B should still have its full quota
        self.assertTrue(limiter.is_allowed(client_b), "Client B should be unaffected by Client A's limit")
        self.assertTrue(limiter.is_allowed(client_b))
        self.assertFalse(limiter.is_allowed(client_b))

    def test_concurrency_and_thread_safety(self):
        """
        Tests the rate limiter under concurrent access from multiple threads
        for the same client, ensuring the limit is not breached.
        """
        requests_per_minute = 100
        limiter = RateLimiter(requests_per_minute=requests_per_minute)
        client_id = "concurrent-client"
        
        num_threads = 20
        requests_per_thread = 10 # Total requests = 200, which is > 100 limit
        
        # A barrier to synchronize the start of all threads
        barrier = Barrier(num_threads)
        
        results = [] # Shared list to store results (True/False)

        def worker():
            barrier.wait() # Wait for all threads to be ready
            for _ in range(requests_per_thread):
                results.append(limiter.is_allowed(client_id))
        
        threads = [Thread(target=worker) for _ in range(num_threads)]
        
        for t in threads:
            t.start()
            
        for t in threads:
            t.join()
            
        allowed_requests = sum(1 for r in results if r is True)
        
        print(f"\n[Concurrency Test] Allowed requests: {allowed_requests} out of {num_threads * requests_per_thread}")
        self.assertEqual(allowed_requests, requests_per_minute, "The number of allowed requests under concurrency must equal the limit")


if __name__ == '__main__':
    unittest.main()