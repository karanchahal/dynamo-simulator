"""
This file details a method that can be used to fire multiple client requests parallely to the same server.
"""
from concurrent import futures
from client_dynamo import client_put, client_get_memory
import time
import random 
from spawn import start_db
from structures import Params

def parallel_runner(num_tasks=4):
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


def test_replication():
    """
    This tests that the server id replicating put requests correctly.
    """
    num_tasks = 2
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)

    # start server
    params = {
        'num_proc' : 4,
        'hash_size': 3, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 2,
        'w_timeout': 2,
        'R': 1,
        'W': 1
    }
    membership_information = {
        0: [1], # key space -> (2,4]
        1: [2], # key space -> (4,6]
        2: [3], # key space -> (6,8]
        3: [0] # key space -> (0,2]
    }
    params = Params(params)
    server = executor.submit(start_db, params, membership_information)

    # fire client request
    ports = [2333,2334,2335,2336]
    start_node = 3 # let's hit node 3 with this put request
    key_val = 2 # this should go into node 0
    port = ports[start_node]

    time.sleep(1)

    client_put(port, 0, key_val)

    # now check that replication has happened correctly
    mem0, repmem0 =  client_get_memory(ports[0])
    mem1, repmem1 =  client_get_memory(ports[1])
    mem2, repmem2 =  client_get_memory(ports[2])
    mem3, repmem3 =  client_get_memory(ports[3])

    # node 0 should have key 2 in it's mem and nothing in it's replicated mem
    assert(key_val in mem0)
    assert(key_val not in mem1 and key_val in repmem1[0].mem)
    assert(key_val not in mem2 and key_val in repmem2[0].mem)
    assert(key_val not in mem3 and 0 not in repmem3)
    print("replication test successful")
    # node 
    # exit()
    # server.shutdown(wait=False)


# parallel_runner()
test_replication()
