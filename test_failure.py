from concurrent import futures
import dynamo_pb2
from dynamo_pb2 import PutRequest
from partitioning import init_membership_list
from client_dynamo import client_put, client_get, client_fail, client_get_memory
import time
from spawn import start_db, start_db_background
from structures import NetworkParams, Params
import time

def test_failure():
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
        'gossip': False
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
    server = start_db_background(params, membership_information, network_params, num_tasks=2)

    time.sleep(1)

    # server = executor.submit(start_db, params, membership_information)
    # fire client request
    s = time.time()
    ports = [2333,2334,2335,2336]
    start_node = 3 # let's hit node 3 with this put request
    key_val = 2 # this should go into node 0
    port = ports[start_node]

    client_fail(2334)
    client_put(port, 0, key_val)

    mem0, repmem0 =  client_get_memory(ports[0])
    mem1, repmem1 =  client_get_memory(ports[1])
    mem2, repmem2 =  client_get_memory(ports[2])
    mem3, repmem3 =  client_get_memory(ports[3])

    # check that mem 3 stores info that should have been in 1
    assert 2 in repmem3[0].mem

    # now fire a get request on node 0 for key 2 and make sure infomraiotn from all replicas is collated.
    response = client_get(port, 0, key_val)

    print(f"Get response {response}")
    e = time.time()
    print(f"Time taken {e - s} secs")

    print('\n------Test failure passed------\n')


test_failure()