from concurrent import futures
import dynamo_pb2
from dynamo_pb2 import PutRequest
# import structures
# import partitioning
# from partitioning import init_membership_list
# from client_dynamo import client_put, client_get, client_fail, client_get_memory
import time
from spawn import start_db, start_db_background
from structures import NetworkParams, Params

def main():
    """
    This tests that the get and put operations are working properly.
    """

    # start server
    params = {
        'num_proc' : 4,
        'hash_size': 3, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 2,
        'w_timeout': 2,
        'r_timeout': 2,
        'R': 1,
        'W': 1
    }
    # membership_information = {
    #     0: [1], # key space -> (2,4]
    #     1: [2], # key space -> (4,6]
    #     2: [3], # key space -> (6,8]
    #     3: [0] # key space -> (0,2]
    # }
    network_params = {
        'latency': 50,
        'randomize_latency': True,
        'drop_prob': 0
    }
    print("constructing...")
    params = Params(params)
    membership_information = init_membership_list(params)
    network_params = NetworkParams(network_params)
    server = start_db_background(params, membership_information, network_params, num_tasks=2)

    # fire client request
    ports = [2333,2334,2335,2336]
    start_node = 3 # let's hit node 3 with this put request
    key = 2 # this should go into node 0
    val = "2"
    key2 = 7 # this should go into node 3
    val2 = "7"
    port = ports[start_node]

    time.sleep(1)

    client_put(port, 0, key, val)
    client_put(port, 0, key2, val2)

    response = client_get(port, 0, key)
    assert response.items[0].val == val
    context = response.items[0].context 

    response = client_get(port, 0, key2)
    assert response.items[0].val == val2

    # Check clock count updation on passing valid context obtained from GET
    client_put(port, 0, key, val2, context=context)
    response = client_get(port, 0, key)
    assert response.items[0].val == val2
    assert response.items[0].context.clock[0].count == 2

    print("\n-------Test get_put passed--------")

def test_failure():
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
        'W': 2
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

    client_fail(2334)
    client_put(port, 0, key_val)

    mem0, repmem0 =  client_get_memory(ports[0])
    mem1, repmem1 =  client_get_memory(ports[1])
    mem2, repmem2 =  client_get_memory(ports[2])
    mem3, repmem3 =  client_get_memory(ports[3])

if __name__ == '__main__':
    # main()
    print("Yo!")
    # test_failure()
