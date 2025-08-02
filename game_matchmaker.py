import unittest
import time
from collections import deque
from threading import Thread, Lock, Event, Condition
from typing import List, Any, Callable, Dict, Set

# =================================================================
#  YOUR IMPLEMENTATION GOES IN THIS CLASS
# =================================================================

class Matchmaker:
    """
    The Problem: A Multiplayer Game Matchmaker

    You are building the matchmaking service for a multiplayer game. The game
    is played by teams of a fixed size. Your task is to implement a
    `Matchmaker` class that groups waiting players into full teams.

    Requirements:
    1.  Initialization: The `Matchmaker` is initialized with:
        * `team_size`: The number of players required for a full team.
        * `timeout_seconds`: The maximum time a player will wait in the
            queue before being removed.
        * `on_team_formed`: A callback function that is invoked with a
            list of `player_id`s when a full team is formed.

    2.  Primary Method: `join_queue(player_id: str)`. This method adds a
        player to the matchmaking queue. It should be thread-safe.

    3.  Team Formation: A team is formed and the `on_team_formed` callback
        is triggered as soon as `team_size` players are in the queue.

    4.  Timeout Logic: If a player waits in the queue for longer than
        `timeout_seconds`, they should be automatically removed from the
        queue. You can assume a simple "player timed out" message can be
        printed or logged; no callback is needed for timeouts.

    5.  State Integrity: A player cannot be in the queue more than once. A
        player who has been placed on a team or has timed out should not be
        part of any future team formations unless they rejoin the queue.

    6.  Shutdown: A `shutdown()` method must be provided to gracefully stop
        any background processes and clean up resources.
    """

    def __init__(self, team_size: int, timeout_seconds: float, on_team_formed: Callable[[List[str]], None]):
        self.team_size = team_size
        self.timeout_seconds = timeout_seconds
        self.on_team_formed = on_team_formed
        self.player_set: Set = set()
        self.shutdown_event = Event()
        self.condition = Condition(Lock())
        self.q = deque()
        self.worker_thread = Thread(target=self.worker_loop, name='MatchmakerTimeoutThread')
        self.worker_thread.start()

    def worker_loop(self):
        while not self.shutdown_event.is_set():
            with self.condition:
                wait_timeout = None
                if self.q:
                    oldest_join_time = self.q[0][1]
                    elapsed = time.time() - oldest_join_time
                    time_to_wait = self.timeout_seconds - elapsed
                    wait_timeout = max(0, time_to_wait)

                self.condition.wait(timeout=wait_timeout)
                # If shutting down, exit the loop immediately.
                if self.shutdown_event.is_set():
                    break

                # wake up (by timeout or notification)
                self.check_queue_and_process()

    def check_queue_and_process(self):
        # assume lock is held
        now = time.time()
        # 1. remove timed out players from the front
        while self.q and (now - self.q[0][1] >= self.timeout_seconds):
            player_id, _ = self.q.popleft()
            self.player_set.remove(player_id)

        # 2. Check if team can be formed
        while len(self.q) >= self.team_size:
            team = []
            for _ in range(self.team_size):
                player_id, _ = self.q.popleft()
                team.append(player_id)
                self.player_set.remove(player_id)
            self.condition.release()
            try:
                self.on_team_formed(team)
            finally:
                self.condition.acquire()
                

    def join_queue(self, player_id: str):
        with self.condition:
            if player_id not in self.player_set:
                self.q.append((player_id,time.time()))
                self.player_set.add(player_id)
                # notify the worker thread that the state has changed
                self.condition.notify()


    def shutdown(self):
        self.shutdown_event.set()
        with self.condition:
            self.condition.notify()
        self.worker_thread.join()



# =================================================================
#  TEST SUITE (DO NOT MODIFY)
# =================================================================

class TestMatchmaker(unittest.TestCase):

    def setUp(self):
        self.formed_teams = []
        self.lock = Lock()
        self.matchmaker = None

    def tearDown(self):
        if self.matchmaker:
            self.matchmaker.shutdown()

    def team_formed_callback(self, team: List[str]):
        with self.lock:
            print(f"Team formed: {team}")
            self.formed_teams.append(sorted(team))

    def test_team_formation_by_size(self):
        """Tests that a team is formed when enough players join."""
        self.matchmaker = Matchmaker(team_size=3, timeout_seconds=5, on_team_formed=self.team_formed_callback)
        self.matchmaker.join_queue("p1")
        self.matchmaker.join_queue("p2")
        self.assertEqual(len(self.formed_teams), 0, "Team should not be formed yet")
        
        self.matchmaker.join_queue("p3")
        time.sleep(0.1)  # Allow time for callback
        
        self.assertEqual(len(self.formed_teams), 1, "A team should have been formed")
        self.assertEqual(self.formed_teams[0], ["p1", "p2", "p3"])

    def test_player_timeout(self):
        """Tests that a player is removed from the queue after a timeout."""
        self.matchmaker = Matchmaker(team_size=2, timeout_seconds=0.2, on_team_formed=self.team_formed_callback)
        self.matchmaker.join_queue("p_timeout")
        
        time.sleep(0.3) # Wait for the player to time out
        
        self.matchmaker.join_queue("p_new_1")
        self.matchmaker.join_queue("p_new_2")
        time.sleep(0.1)

        self.assertEqual(len(self.formed_teams), 1, "A new team should form without the timed-out player")
        self.assertNotIn("p_timeout", self.formed_teams[0], "Timed-out player should not be in the new team")
        self.assertEqual(self.formed_teams[0], ["p_new_1", "p_new_2"])

    def test_no_duplicate_players_in_queue(self):
        """Tests that adding the same player twice doesn't break the queue."""
        self.matchmaker = Matchmaker(team_size=2, timeout_seconds=5, on_team_formed=self.team_formed_callback)
        self.matchmaker.join_queue("p_dup")
        self.matchmaker.join_queue("p_dup") # Second attempt
        self.matchmaker.join_queue("p_other")
        time.sleep(0.1)
        
        self.assertEqual(len(self.formed_teams), 1, "A team should form with unique players")
        self.assertEqual(self.formed_teams[0], ["p_dup", "p_other"])

    def test_concurrency_and_no_player_loss(self):
        """Tests that all players are correctly placed into teams under high concurrent load."""
        team_size = 5
        num_threads = 20
        players_per_thread = 10
        total_players = num_threads * players_per_thread
        
        self.matchmaker = Matchmaker(team_size=team_size, timeout_seconds=2, on_team_formed=self.team_formed_callback)
        
        all_submitted_players = set()
        
        def worker(thread_id):
            for i in range(players_per_thread):
                player_id = f"t{thread_id}-p{i}"
                with self.lock:
                    all_submitted_players.add(player_id)
                self.matchmaker.join_queue(player_id)
                time.sleep(0.001) # Small sleep to vary join times

        threads = [Thread(target=worker, args=(i,)) for i in range(num_threads)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        time.sleep(0.2) # Allow final teams to form
        self.matchmaker.shutdown()

        with self.lock:
            all_team_members = set(player for team in self.formed_teams for player in team)
            print(f"\n[Concurrency Test] Total submitted: {len(all_submitted_players)}, Total placed in teams: {len(all_team_members)}")
            
            self.assertEqual(len(all_team_members), total_players, "All players should be placed in a team")
            self.assertEqual(all_team_members, all_submitted_players, "The set of players in teams must match the set of submitted players")
            self.assertEqual(len(self.formed_teams), total_players // team_size)

    def test_shutdown_cleans_up(self):
        """Tests that the background thread is stopped after shutdown."""
        self.matchmaker = Matchmaker(team_size=2, timeout_seconds=0.1, on_team_formed=self.team_formed_callback)
        # Assuming a background thread for timeouts, check if it's alive
        # This is implementation-dependent, but a common pattern.
        timeout_thread = None
        for thread in threading.enumerate():
            if thread.name == "MatchmakerTimeoutThread":
                timeout_thread = thread
                break
        
        self.assertIsNotNone(timeout_thread, "A timeout thread should be running")
        self.assertTrue(timeout_thread.is_alive())

        self.matchmaker.shutdown()
        time.sleep(0.2) # Give time for thread to exit

        self.assertFalse(timeout_thread.is_alive(), "Timeout thread should be stopped after shutdown")


if __name__ == '__main__':
    # A little hack to make the last test work by giving the thread a name
    import threading
    original_thread_init = threading.Thread.__init__
    def new_init(self, *args, **kwargs):
        if 'target' in kwargs and kwargs['target'].__name__ == '_timeout_checker_loop':
             kwargs['name'] = 'MatchmakerTimeoutThread'
        original_thread_init(self, *args, **kwargs)
    threading.Thread.__init__ = new_init
    
    unittest.main()
