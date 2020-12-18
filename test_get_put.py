from concurrent import futures
import dynamo_pb2
from dynamo_pb2 import PutRequest
from partitioning import init_membership_list
from client_dynamo import client_put, client_get, client_fail, client_get_memory
import time
from spawn import start_db, start_db_background
from structures import NetworkParams, Params
import logging 

def main():
    """
    This tests that the get and put operations are working properly.
    """
    logging.basicConfig(filename='get_put.log', level=logging.DEBUG)
    logger = logging.getLogger('get_put.log')
    # start server
    params = {
        'num_proc' : 8,
        'hash_size': 8, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 2, # actually N-1, excluding coordinator node
        'w_timeout': 2,
        'r_timeout': 2,
        'R': 2,
        'W': 2,
        'gossip': False,
        'update_failure_on_rpcs': True
    }

    network_params = {
        'latency': 0,
        'randomize_latency': False,
        'drop_prob': 0
    }
    print("constructing...")
    params = Params(params)
    membership_information = init_membership_list(params)
    network_params = NetworkParams(network_params)
    server = start_db_background(params, membership_information, network_params, num_tasks=2, logger=logger)

    time.sleep(1)

    # fire client request
    ports = [2333,2334,2335,2336]
    start_node = 3 # let's hit node 3 with this put request
    key = 2 # this should go into node 0
    val = "2"
    key2 = 7 # this should go into node 3
    val2 = "7"
    port = ports[start_node]
    client_put(port, 0, key, val)
    client_put(port, 0, key2, val2)


    mem0, repmem0 =  client_get_memory(ports[0])
    mem1, repmem1 =  client_get_memory(ports[1])
    mem2, repmem2 =  client_get_memory(ports[2])
    mem3, repmem3 =  client_get_memory(ports[3])

    s = time.time()
    response = client_get(port, 0, key)
    e = time.time()
    print(f"GET {e - s} secs")
    print(f"Get response {response}")
    assert response.items[0].val == val
    context = response.items[0].context 

    response = client_get(port, 0, key2)
    assert response.items[0].val == val2

    # Check clock count updation on passing valid context obtained from GET
    s = time.time()
    client_put(port, 0, key, val2, context=context)
    e = time.time()
    print(f"PUT {e - s} secs")
    response = client_get(port, 0, key)
    assert response.items[0].val == val2
    assert response.items[0].context.clock[0].count == 2

    print("\n-------Test get_put passed--------\n")


if __name__ == '__main__':
    main()