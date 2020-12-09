"""
This file details a method that can be used to fire multiple client requests parallely to the same server.
"""
import numpy as np
from concurrent import futures
from client_dynamo import client_put, client_get_memory
import time
import random 
from spawn import start_db
from structures import Params

def parallel_runner_old(num_tasks=4):
    s = time.time()
    executor = futures.ThreadPoolExecutor(max_workers=1)
    fut = set([])
    ports = [2333,2334,2335,2336]

    for i in range(1000):
        key_val = random.randint(0,7) # assuming key space is of size 8
        port = ports[random.randint(0,3)]
        fut.add(executor.submit(client_get_memory, port))

    done, not_done = futures.wait(fut)
    e = time.time()
    # print(done)
    print(f"Time taken : {e - s} secs")
    print(f"Pending requests {not_done}")


def timed(func):
    def _w(*a, **k):
        then = time.time()
        res = func(*a, **k)
        elapsed = time.time() - then
        return elapsed, res
    return _w

def run_parallel(requests, requests_params, key=1, val="1", start_port=2333):
    # start = time.time()
    executor = futures.ThreadPoolExecutor(max_workers=1)
    fut = set([])
    for request, request_params in zip(requests, requests_params):
        fut.add(executor.submit(timed(request), **request_params))

    durations = []
    for it in futures.as_completed(fut, timeout=100):
        duration, _ = it.result()
        durations.append(duration)

    return np.array(durations)