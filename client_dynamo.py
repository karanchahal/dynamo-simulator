
"""
This file 
"""

import time
import grpc

from dynamo_pb2_grpc import DynamoInterfaceStub
from dynamo_pb2 import GetRequest, GetResponse, PutRequest, PutResponse, VectorClock, VectorClockItem

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

def put(stub, client_id, key, val, context):
    """
    Regular get request
    """
    request = PutRequest(client_id=client_id, key=key, val=val, context=context)
    response : PutResponse = stub.Put(request)
    print(f"Put Response recieved from {response.server_id}")


def client_get(server_address, client_id):
    with grpc.insecure_channel(server_address) as channel:
        stub = DynamoInterfaceStub(channel)
        get(stub, client_id, 1)


def client_put(server_address, client_id):
    with grpc.insecure_channel(server_address) as channel:
        stub = DynamoInterfaceStub(channel)
        item = VectorClockItem(server_id=1, count=1)
        context = VectorClock(clock=[item])
        put(stub, client_id, key=1, val="1", context=context)


client_put("localhost:2333", 1)
