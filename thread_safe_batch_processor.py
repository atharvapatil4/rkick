import unittest
import time
import threading
from threading import Thread, Lock, Condition, Event
from typing import List, Any, Callable
from collections import deque

# =================================================================
#  YOUR IMPLEMENTATION GOES IN THIS CLASS
# =================================================================

class BatchProcessor:
    """
    The Problem: A Thread-Safe Asynchronous Batch Processor

    In many high-throughput systems (like logging, analytics, or event
    streaming), it's inefficient to process every single item individually.
    A common pattern is to buffer items into batches and process them together.

    Your task is to implement a BatchProcessor class. This class will collect
    items submitted from multiple threads and process them in batches based
    on either size or a timeout.

    Requirements:
    1. The BatchProcessor is initialized with:
        - `batch_size`: The maximum number of items in a batch.
        - `timeout_seconds`: The maximum time to wait before processing an
          incomplete batch, starting from when the *first* item of that
          batch was added.
        - `processing_callback`: A function that will be called with a list
          of items when a batch is ready.

    2. It must have one primary method: `submit(item: any)`. This method
       adds an item to the current batch and is expected to be called from
       many threads concurrently.

    3. A batch is processed (by calling the `processing_callback`) when
       either of these conditions is met first:
        - The number of items in the batch reaches `batch_size`.
        - The time elapsed since the first item was added to the batch
          exceeds `timeout_seconds`.

    4. The implementation must be thread-safe. Race conditions must be
       prevented (e.g., a batch being processed by both size and timeout
       simultaneously).

    5. It must have a `shutdown()` method. This method should ensure any
       final, pending items are processed before stopping, and it should
       clean up any background threads gracefully.
    """

    def __init__(self, batch_size: int, timeout_seconds: float, processing_callback: Callable[[List[Any]], None]):
        """
        Initializes the BatchProcessor.

        Args:
            batch_size: The max number of items per batch.
            timeout_seconds: The max time to wait before processing an incomplete batch.
            processing_callback: The function to call with a processed batch.
        """
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.processing_callback = processing_callback

        self.condition = Condition(Lock())
        self.batch = []
        self.first_item_timestamp = None

        self.shutdown_event = Event()

        self.worker_thread = Thread(target=self._worker_loop)
        self.worker_thread.start()

    def _worker_loop(self):
        while not self.shutdown_event.is_set():
            with self.condition:
                timeout = None
                # batch has started
                if self.first_item_timestamp:
                    elapsed = time.time() - self.first_item_timestamp
                    if elapsed < self.timeout_seconds:
                        timeout = self.timeout_seconds - elapsed
                    else:
                        timeout = 0 # timeout has already happened

                # wait for notification or for timeout to expire
                # releases lock, reacquires when waking up
                # CPU: The correct solution is to put the thread to sleep until there is actually work to do.
                self.condition.wait(timeout=timeout)

                # after waking up, check conditions for processing batch

                if self.shutdown_event.is_set():
                    break

                if self.batch:
                    is_full = len(self.batch) >= self.batch_size
                    time_out = self.first_item_timestamp and (time.time() - self.first_item_timestamp >= self.timeout_seconds)
                    if is_full or time_out:
                        self._process_batch()


        with self.condition:
            if self.batch:
                self._process_batch()

    def _process_batch(self):
        batch = self.batch.copy()
        self.batch.clear()
        self.first_item_timestamp = None

        self.condition.release()
        try:
            self.processing_callback(batch)
        finally:
            self.condition.acquire()


    def submit(self, item: Any):
        """
        Submits an item to be processed in a future batch.
        This method must be thread-safe.
        """
        if self.shutdown_event.is_set():
            return
        with self.condition:
            is_first_item = False
            if not self.batch:
                is_first_item=True
                self.first_item_timestamp = time.time() # first one

            self.batch.append(item)

            if len(self.batch) >= self.batch_size or is_first_item:
                self.condition.notify() # wake up!


    def shutdown(self):
        """
        Processes any remaining items and stops the processor.
        Ensures graceful termination of any background threads.
        """
        if not self.shutdown_event.is_set():
            self.shutdown_event.set()
            with self.condition:
                self.condition.notify()

            self.worker_thread.join()




# =================================================================
#  TEST SUITE (DO NOT MODIFY)
# =================================================================

class TestBatchProcessor(unittest.TestCase):

    def setUp(self):
        self.processed_batches = []
        self.processor = None

    def tearDown(self):
        if self.processor:
            self.processor.shutdown()

    def callback(self, batch: List[Any]):
        print(f"Processing batch of size {len(batch)}: {batch}")
        self.processed_batches.append(batch)

    def test_batching_by_size(self):
        """Tests that a full batch is processed when it reaches the specified size."""
        self.processor = BatchProcessor(batch_size=3, timeout_seconds=5, processing_callback=self.callback)
        self.processor.submit(1)
        self.processor.submit(2)
        self.assertEqual(len(self.processed_batches), 0, "Batch should not be processed before it's full")
        
        self.processor.submit(3)
        time.sleep(0.1) # Allow time for processing thread to run
        
        self.assertEqual(len(self.processed_batches), 1, "Batch should be processed once full")
        self.assertEqual(self.processed_batches[0], [1, 2, 3])

        self.processor.submit(4)
        self.processor.submit(5)
        self.processor.submit(6)
        time.sleep(0.1)

        self.assertEqual(len(self.processed_batches), 2, "A second full batch should be processed")
        self.assertEqual(self.processed_batches[1], [4, 5, 6])

    def test_batching_by_timeout(self):
        """Tests that an incomplete batch is processed after the timeout."""
        self.processor = BatchProcessor(batch_size=10, timeout_seconds=0.2, processing_callback=self.callback)
        self.processor.submit("a")
        self.processor.submit("b")
        
        # Wait for a time longer than the timeout
        time.sleep(0.3)
        
        self.assertEqual(len(self.processed_batches), 1, "Incomplete batch should be processed after timeout")
        self.assertEqual(self.processed_batches[0], ["a", "b"])
        
        # Submit another item to start a new batch
        self.processor.submit("c")
        time.sleep(0.3)
        
        self.assertEqual(len(self.processed_batches), 2, "A second incomplete batch should be processed after timeout")
        self.assertEqual(self.processed_batches[1], ["c"])

    def test_shutdown_processes_pending_items(self):
        """Tests that shutdown processes any remaining items."""
        self.processor = BatchProcessor(batch_size=10, timeout_seconds=5, processing_callback=self.callback)
        self.processor.submit("final_item_1")
        self.processor.submit("final_item_2")
        
        self.processor.shutdown()
        
        self.assertEqual(len(self.processed_batches), 1, "Shutdown should process pending items")
        self.assertEqual(self.processed_batches[0], ["final_item_1", "final_item_2"])
        
        # Test that submitting after shutdown does nothing
        self.processor.submit("post_shutdown_item")
        time.sleep(0.1)
        self.assertEqual(len(self.processed_batches), 1, "Should not process items submitted after shutdown")

    def test_concurrency_and_data_integrity(self):
        """
        Tests that all items are processed exactly once under high concurrent load.
        """
        batch_size = 50
        num_threads = 10
        items_per_thread = 100
        total_items = num_threads * items_per_thread

        self.processor = BatchProcessor(batch_size=batch_size, timeout_seconds=0.1, processing_callback=self.callback)
        
        all_submitted_items = set()

        def worker(thread_id):
            for i in range(items_per_thread):
                item = f"t{thread_id}-{i}"
                all_submitted_items.add(item)
                self.processor.submit(item)
        
        threads = [Thread(target=worker, args=(i,)) for i in range(num_threads)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        self.processor.shutdown()
        
        all_processed_items = set(item for batch in self.processed_batches for item in batch)
        
        print(f"\n[Concurrency Test] Total submitted: {len(all_submitted_items)}, Total processed: {len(all_processed_items)}")
        
        self.assertEqual(len(all_submitted_items), total_items)
        self.assertEqual(all_processed_items, all_submitted_items, "All submitted items must be processed exactly once")


if __name__ == '__main__':
    unittest.main()
