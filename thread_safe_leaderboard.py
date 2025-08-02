import heapq
from fastapi import FastAPI, HTTPException, Query

"""
1. 
Let's design a system for a real-time gaming leaderboard. 
We need to be able to submit scores for players and retrieve the top N players at any given moment.
"""
import heapq
from collections import defaultdict
# Naive solution
class Leaderboard:

    def __init__(self, n):
        self.n = n
        self.players = defaultdict(int)

    def update_score(self, user_id, score):
        self.players[user_id] = score


    def get_top_players(self):
        # min heap of size n
        top_n_heap = []
        for player_id, score in self.players.items():
            #(log(k))
            if len(top_n_heap) < self.n:
                heapq.heappush(top_n_heap, (score, player_id))
            else:
                # check if curr score larger than smallest
                smallest = top_n_heap[0]
                if score > smallest:
                    heapq.heapreplace(top_n_heap(score,player_id))
        
        ans = sorted(top_n_heap, key = lambda x: x[0], reverse=True)
        return [player_id for score, player_id in ans]

"""
2.
What if this Leaderboard is part of a web service, accessed by 1000s of players at once?
Multiple game servers are calling add_score concurrently.

"""
# Thread-safe solution
import threading
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List

class ThreadSafeLeaderboard:
    def __init__(self, n):
        self.n = n
        self.players = defaultdict(int)
        self.lock = threading.Lock()

    def update_score(self, user_id, score):
        with self.lock:
            self.players[user_id] = score

    def get_top_players(self):
        # copy locally
        with self.lock:
            players = self.players.copy()
        top_n_heap = []
        for player_id, score in self.players.items():
            #(log(k))
            if len(top_n_heap) < self.n:
                heapq.heappush(top_n_heap, (score, player_id))
            else:
                # check if curr score larger than smallest
                smallest = top_n_heap[0]
                if score > smallest:
                    heapq.heapreplace(top_n_heap(score,player_id))
        
        ans = sorted(top_n_heap, key = lambda x: x[0], reverse=True)
        return ans

app = FastAPI(
    title="game",
    description="",
) 

# Data Models
l = ThreadSafeLeaderboard()
class ScoreSubmitRequest(BaseModel):
    player_id: str
    score:int

class LeaderboardResponseElem(BaseModel):
    player_id: str
    score:int
class LeaderboardResponse(BaseModel):
    leaderboard: List[LeaderboardResponseElem]

# API Routes
@app.post("v1/scores", status_code=202)
def update_score(request:ScoreSubmitRequest):
    l.update_score(request.player_id, request.score)

@app.get("/v1/leaderboard")
def get_leaderboard():
    if not l.players:
        return {"leaderboard": {}}
    
    top = l.get_top_players()
    formatted = [LeaderboardResponseElem(id, score) for id,score in top]
    return {"leaderboard": formatted}

"""
3. even higher traffic
API server consumes from kafka for score updates
client req --> KAFKA --> unqueued via task workers. Redis writes
Instead of in-memory leaderboard, use redis (Redis can ocassionally be backed to disk at some freq)
use Redis sorted set 
    A. Adding or Updating a Score: ZADD key score member
    B. Getting top players; ZREVRANGE key start stop [WITHSCORES]
    C. RDB snapshot vs Append-Only-File
        - AOF allows "replayability". Can have granular control over how often to refresh
"""




