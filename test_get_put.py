from concurrent import futures
from client_dynamo import client_put, client_get
import time
import random 
from spawn import start_db
from structures import Params

def test_get_put():
    """
    This tests that the get and put operations are working properly.
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
        'r_timeout': 2,
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

    print("-----Test get_put passed")

test_get_put()
