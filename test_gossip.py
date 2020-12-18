from concurrent import futures
import dynamo_pb2
from dynamo_pb2 import PutRequest
from partitioning import init_membership_list
from client_dynamo import client_put, client_get, client_fail, client_get_memory, client_gossip
import time
from spawn import start_db, start_db_background
from structures import NetworkParams, Params
import time
import logging

logger = logging.getLogger('dynamo_node')
logger.setLevel(logging.INFO)

def test_gossip():
    logging.basicConfig(filename='gossip.log', level=logging.DEBUG)
    logger = logging.getLogger('gossip.log')

    num_tasks = 2
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)

    # start server
    params = {
        'num_proc' : 4,
        'hash_size': 3, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 3,
        'w_timeout': 2,
        'r_timeout': 2,
        'R': 3,
        'W': 3,
        'gossip' : False,
        'update_failure_on_rpcs': False
    }
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

    params = Params(params)

    network_params = NetworkParams(network_params)
    server = start_db_background(params, membership_information, network_params, num_tasks=2, wait=True, logger=logger)

    time.sleep(1)

    # # server = executor.submit(start_db, params, membership_information)
    # # fire client request
    # s = time.time()
    ports = [2333,2334,2335,2336]
    start_node = 3 # let's hit node 3 with this put request
    key_val = 2 # this should go into node 0
    port = ports[start_node]

    client_fail(2334)
    client_put(port, 0, key_val)

    # print memory of node 1 and 3
    mem1, repmem1 =  client_get_memory(ports[1])
    mem3, repmem3 =  client_get_memory(ports[3])

    # replication of key 2 at node 0, at node 2 and 3. Node 1's hinted handoff in 3
    # turn gossip on

    client_fail(2334, fail=False) # unfail

    client_gossip(2336)

    # wait for some time and check memory of 3 and 1 again

    time.sleep(2)

    mem1_update, repmem1_update =  client_get_memory(ports[1])
    mem3_update, repmem3_update =  client_get_memory(ports[3])

    assert(key_val not in repmem1[0].mem and key_val in repmem1_update[0].mem) # key is now there

    assert(key_val in repmem3[0].mem and key_val not in repmem3_update[0].mem)

    print(f"Test gossip has passed")

if __name__ == "__main__":
    test_gossip()