
"""
This file 
"""

import time
import grpc

from dynamo_pb2_grpc import DynamoInterfaceStub
from dynamo_pb2 import GetRequest, GetResponse, PutRequest, PutResponse, VectorClock, VectorClockItem, NoParams, FailRequest

def bidirectional_get(stub, client_id):
    """
    The stub contains a method which can give the client a response iterator
    it can use to get the responses from the server. In tuern it has to send
    a request iterator to the server, so that it can iterate through all the requests.
    """
    raise NotImplementedError

def get(stub, client_id, key):
    """
    Regular get request
    """
    request = GetRequest(client_id=client_id, key=key)
    response : GetResponse = stub.Get(request)
    print(f"Get Response recieved from {response.server_id}")
    return response

def put(stub, request: PutRequest):
    """
    Regular put request
    """
    response : PutResponse = stub.Put(request)
    print(f"Put Response recieved from {response.server_id}")
    return response


def client_get(port, client_id, key=1):
    print("-------------Sending GET request !!!--------------")
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = get(stub, client_id, key)

    if response.reroute == True:
        # sending to actual coordinator node
        with grpc.insecure_channel(f"localhost:{response.reroute_server_id}") as channel:
            stub = DynamoInterfaceStub(channel)
            response = get(stub, client_id, key)

    return response

def client_put(port, client_id, key=1, val="1", context=None):
    # item = VectorClockItem(server_id=1, count=1)
    if context is None:
        context = VectorClock(clock=[]) # An existing context only needs to be passed when updating an existing key's value
    request = PutRequest(client_id=client_id, key=key, val=val, context=context, hinted_handoff=-1)
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = put(stub, request)
    
    if response.reroute == True:
        # sending it to actual coordinator node
        with grpc.insecure_channel(f"localhost:{response.reroute_server_id}") as channel:
            stub = DynamoInterfaceStub(channel)
            response = put(stub, request)
    
    return response

def client_get_memory(port):
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        request = NoParams()
        response = stub.PrintMemory(request)
    return response.mem, response.mem_replicated

def client_fail(port, fail=True):
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        request = FailRequest(fail=fail)
        response = stub.Fail(request)
# client_put(2333, 1)
