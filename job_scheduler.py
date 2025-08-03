import unittest
import time
import threading
import uuid
import heapq
import queue
from collections import deque
from threading import Thread, Lock, Event, Condition
from typing import List, Callable, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================
#  YOUR IMPLEMENTATION GOES IN THIS CLASS
# =================================================================

class JobScheduler:
    """
    A Thread-Safe Job Scheduler

    You are tasked with building a `JobScheduler` class that can schedule
    functions (jobs) to be executed at some point in the future. The
    scheduler must be able to handle submissions from multiple threads
    concurrently and execute the jobs using a pool of worker threads.

    Requirements:

    1.  Initialization: The scheduler is initialized with `num_workers`,
        which defines the size of the internal worker thread pool that
        will execute the jobs.
        - `__init__(self, num_workers: int)`

    2.  Scheduling: The scheduler must provide a method to schedule a
        no-argument function (`func`) to run after a specified `delay_seconds`.
        - This method must be thread-safe.
        - It must return a unique `job_id` (string) that can be used to
          reference the job later.
        - `schedule(self, func: Callable[[], None], delay_seconds: float) -> str`

    3.  Cancellation: The scheduler must allow a *pending* (not yet executing)
        job to be canceled using its `job_id`.
        - If a job is successfully found and canceled, the method should
          return `True`.
        - If the job cannot be canceled (because it's already running,
          completed, or the ID is invalid), it should return `False`.
        - `cancel(self, job_id: str) -> bool`

    4.  Execution:
        - Jobs must be executed by the worker threads, not by the main
          scheduler thread. This ensures the scheduler itself remains
          responsive.
        - Jobs scheduled with a shorter delay should run before jobs
          scheduled with a longer delay.

    5.  Shutdown:
        - The scheduler needs a `shutdown()` method for graceful termination.
        - When called, it should stop accepting new jobs.
        - It should allow any jobs that are *currently executing* to finish.
        - All *pending* jobs that have not yet started should be discarded.
        - All background threads (the scheduler and all workers) must be
          stopped cleanly.
    """

    def __init__(self, num_workers: int):
        """
        Initializes the JobScheduler with a pool of worker threads.
        """
        # need priority queue
        self.task_queue = queue.Queue() # Thread safe
        self.num_workers = num_workers
        self.condition = Condition()
        self.shutdown_event = Event()
        self.canceled_jobs = set()
        self.pending_jobs = set()
        self.heap = [] # (runtime, job_id, func)
        self.worker_threads: list[Thread] = []
        self.scheduler_thread = Thread(target=self._scheduler_loop, name=f"scheduler")
        self.scheduler_thread.start()
        for i in range(num_workers):
            thread = Thread(target = self._worker_loop, name=f"t{i}")
            self.worker_threads.append(thread)
            thread.start()

    def _scheduler_loop(self):
        """
        Watches heap and pushes tasks onto the queue (consumed by worker threads)
        """
        while not self.shutdown_event.is_set():
            with self.condition:
                if not self.heap:
                    self.condition.wait()
                    continue
                next_run_time, _, _ = self.heap[0]
                time_to_wait = next_run_time - time.time()

                if time_to_wait > 0:
                    self.condition.wait(time_to_wait)
                    continue # reavaluate the heap from top
                _, job_id, func = heapq.heappop(self.heap)
                self.pending_jobs.remove(job_id)

                if job_id in self.canceled_jobs:
                    self.canceled_jobs.remove(job_id)
                    continue

                self.task_queue.put((job_id, func))



    # Concept: worker thread does NOT do the sleep 
    def _worker_loop(self):
        while True:
            job = self.task_queue.get()
            if job is None:
                break
            job_id, func = job
            with self.condition:
                if job_id in self.canceled_jobs:
                    self.canceled_jobs.remove(job_id)
                    self.task_queue.task_done()
                    continue
            try:
                func()
            finally:
                self.task_queue.task_done()


    def schedule(self, func: Callable[[], None], delay_seconds: float) -> str:
        """
        Schedules a function to be executed after a delay.
        """
        job_id = str(uuid.uuid4())
        t = time.time() + delay_seconds
        with self.condition:
            task = (t, job_id, func)
            self.pending_jobs.add(job_id)
            heapq.heappush(self.heap, task)
            self.condition.notify()
        return job_id


    def cancel(self, job_id: str) -> bool:
        """
        Cancels a pending job.
        """
        
        with self.condition:
            if job_id in self.pending_jobs:
                self.canceled_jobs.add(job_id)
                self.condition.notify()
                return True
            return False
            

    def shutdown(self):
        """
        Shuts down the scheduler, stopping all threads gracefully.
        """
        self.shutdown_event.set()
        with self.condition:
            self.condition.notify()

        self.scheduler_thread.join()
        for t in self.worker_threads:
            self.task_queue.put(None)

        for t in self.worker_threads:
            t.join() 

# =================================================================
#  TEST SUITE (DO NOT MODIFY)
# =================================================================

class TestJobScheduler(unittest.TestCase):#

    def setUp(self):
        self.scheduler = None
        self.executed_jobs_info = []
        self.lock = Lock()

    def tearDown(self):
        if self.scheduler:
            self.scheduler.shutdown()

    def job_callback(self, job_id, duration=0):
        """A sample job that records its execution."""
        with self.lock:
            print(f"Executing job {job_id} at {time.time():.2f}")
            self.executed_jobs_info.append({
                "job_id": job_id,
                "execution_time": time.time(),
            })
        if duration > 0:
            time.sleep(duration)
        with self.lock:
            print(f"Finished job {job_id}")

    def test_schedule_and_run_single_job(self):
        """Tests that a single job is scheduled and executed after its delay."""
        self.scheduler = JobScheduler(num_workers=1)
        start_time = time.time()
        job_id = self.scheduler.schedule(lambda: self.job_callback("job1"), 0.1)
        
        time.sleep(0.2)
        self.scheduler.shutdown()

        self.assertEqual(len(self.executed_jobs_info), 1)
        self.assertEqual(self.executed_jobs_info[0]["job_id"], "job1")
        execution_delay = self.executed_jobs_info[0]["execution_time"] - start_time
        self.assertAlmostEqual(execution_delay, 0.1, delta=0.05)

    def test_job_execution_order(self):
        """Tests that jobs are executed in the order of their scheduled time."""
        self.scheduler = JobScheduler(num_workers=1)
        job_id_1 = self.scheduler.schedule(lambda: self.job_callback("job_late"), 0.2)
        job_id_2 = self.scheduler.schedule(lambda: self.job_callback("job_early"), 0.1)

        time.sleep(0.3)
        self.scheduler.shutdown()

        self.assertEqual(len(self.executed_jobs_info), 2)
        self.assertEqual(self.executed_jobs_info[0]["job_id"], "job_early")
        self.assertEqual(self.executed_jobs_info[1]["job_id"], "job_late")
        
    def test_cancel_pending_job(self):
        """Tests that a pending job can be successfully canceled."""
        self.scheduler = JobScheduler(num_workers=1)
        job_id = self.scheduler.schedule(lambda: self.job_callback("job_cancel"), 0.2)
        
        time.sleep(0.05)
        was_canceled = self.scheduler.cancel(job_id)
        self.assertTrue(was_canceled)
        
        time.sleep(0.2) # Wait past the original execution time
        self.scheduler.shutdown()

        self.assertEqual(len(self.executed_jobs_info), 0, "Canceled job should not have executed")

    def test_cannot_cancel_running_or_completed_job(self):
        """Tests that a job cannot be canceled once it has started running."""
        self.scheduler = JobScheduler(num_workers=1)
        job_id = self.scheduler.schedule(lambda: self.job_callback("job_running", duration=0.2), 0.1)

        time.sleep(0.15) # Wait for the job to start executing
        was_canceled = self.scheduler.cancel(job_id)
        self.assertFalse(was_canceled, "Should not be able to cancel a job that is already running")
        
        self.scheduler.shutdown() # Shutdown will wait for the job to complete
        self.assertEqual(len(self.executed_jobs_info), 1, "Job should have run to completion")

    def test_shutdown_discards_pending_jobs(self):
        """Tests that shutdown discards pending jobs."""
        self.scheduler = JobScheduler(num_workers=1)
        job_id_exec = self.scheduler.schedule(lambda: self.job_callback("job_executing", duration=0.2), 0.01)
        job_id_pending = self.scheduler.schedule(lambda: self.job_callback("job_pending"), 0.3)

        time.sleep(0.05) # Let the first job start
        self.scheduler.shutdown()
        
        self.assertEqual(len(self.executed_jobs_info), 1, "Only the first job should execute")
        self.assertEqual(self.executed_jobs_info[0]["job_id"], "job_executing")
        
    def test_concurrency_and_cancellation(self):
        """Tests scheduling and cancellation from multiple threads."""
        num_threads = 10
        jobs_per_thread = 20
        self.scheduler = JobScheduler(num_workers=4)
        
        jobs_to_cancel = set()
        all_job_ids = set()
        
        def task(thread_id):
            for i in range(jobs_per_thread):
                # The lambda needs to capture the job_id by value, hence `j=...`
                job_id = self.scheduler.schedule(lambda j=f"t{thread_id}-j{i}": self.job_callback(j), 0.1 + i * 0.01)
                with self.lock:
                    all_job_ids.add(job_id)
                    # Mark every 4th job for cancellation
                    if i % 4 == 0:
                        jobs_to_cancel.add(job_id)

        threads = [Thread(target=task, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Concurrently cancel jobs
        for job_id in list(jobs_to_cancel):
            self.scheduler.cancel(job_id)
        
        time.sleep(0.1 + jobs_per_thread * 0.01 + 0.1) # Wait for all jobs to have a chance to run
        self.scheduler.shutdown()
        
        executed_job_ids = {info["job_id"] for info in self.executed_jobs_info}
        expected_executed_ids = all_job_ids - jobs_to_cancel
        
        self.assertEqual(executed_job_ids, expected_executed_ids, "Executed jobs should match non-canceled jobs")
        self.assertEqual(len(executed_job_ids), len(all_job_ids) - len(jobs_to_cancel))

if __name__ == '__main__':
    unittest.main(verbosity=2)