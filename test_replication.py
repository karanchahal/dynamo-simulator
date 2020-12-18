"""
This file details a method that can be used to fire multiple client requests parallely to the same server.
"""
from concurrent import futures
from client_dynamo import client_put, client_get_memory
import time
import random 
from spawn import start_db, start_db_background
from structures import Params, NetworkParams
import logging

def test_replication():
    """
    This tests that the server id replicating put requests correctly.
    """

    logging.basicConfig(filename='replication.log', level=logging.DEBUG)
    logger = logging.getLogger('replication.log')

    num_tasks = 2
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)

    # start server
    params = {
        'num_proc' : 4,
        'hash_size': 3, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 3,
        'w_timeout': 2,
        'R': 1,
        'W': 2,
        'gossip': False,
        'update_failure_on_rpcs': False
    }
    
    membership_information = {
        0: [1], # key space -> (2,4]
        1: [2], # key space -> (4,6]
        2: [3], # key space -> (6,8]
        3: [0] # key space -> (0,2]
    }
    params = Params(params)

    # start server

    membership_information = {
        0: [1], # key space -> (2,4]
        1: [2], # key space -> (4,6]
        2: [3], # key space -> (6,8]
        3: [0] # key space -> (0,2]
    }

    network_params = {
        'latency': 0,
        'randomize_latency': False,
        'drop_prob': 0
    }
    network_params = NetworkParams(network_params)
    server = start_db_background(params, membership_information, network_params, num_tasks=2, wait=False, logger=logger)

    # server = executor.submit(start_db, params, membership_information)

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



test_replication()
