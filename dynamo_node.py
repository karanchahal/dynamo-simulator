
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict
from dynamo_pb2 import PutResponse, GetResponse, PutRequest, GetRequest, ReplicateResponse
from partitioning import get_preference_list, createtoken2node
from structures import Params
from dynamo_pb2_grpc import DynamoInterfaceStub
import grpc

class DynamoNode(DynamoInterfaceServicer):
    """
    This class is the entry point to a dynamo node.
    """
    def __init__(self, n_id: int, view: Dict[int, str], 
        membership_information: Dict[int, List[int]], params: Params):
        """
        view: dict indexed by node id, value is the address of the node
        membership_information: dict indexed by node id, gives set of tokens for each node. 
            Each token states the starting point of a virtual node which is of size `Q` (see `Params`).
        params: Stores config information of the dynamo ring
        """
        super().__init__()

        self.params = params

        self.n_id = n_id

        # tokens given to various nodes in the ring: bootstrapped information
        self.membership_information = membership_information

        # list of address to other nodes, indexed by node id
        self.view = view

        # a list of > N nodes that are closest to current node, (clockwise)
        self.preference_list = get_preference_list(n_id=n_id, membership_info=membership_information, params=params)

        self.token2node = createtoken2node(membership_information)

        # in memory data store of key, values
        self.memory: Dict[PutRequest] = {}


    def Get(self, request: GetRequest, context):
        """
        Get request
        """
        print(f"get called by client({request.client_id}) for key: {request.key}")
        response = GetResponse(
            server_id=self.n_id,
            val="1",
            context="None",
            metadata="Yo")
        return response


    def Put(self, request: PutRequest , context):
        """
        Put Request
        """
        print(f"Put called by {request}")

        # add to memory
        response: PutResponse = self._add_to_memory(request, request_type="put")
        
        print(f"Put sending a response back {response}")
        return response
    
    def Replicate(self, request: PutRequest , context):
        """
        Put Request
        """
        print(f"Put called by {request}")

        # add to memory
        self._add_to_hash_table(request)

        # construct replicate response
        response = ReplicateResponse(server_id=self.n_id, 
                    metadata="Replication successful", 
                    succ=True)
        
        print(f"Put sending a response back {response}")
        return response
    
    def _add_to_hash_table(self, request):
        """
        Adds request to hash table.
        """
        self.memory[request.key] = request

    def _add_to_memory(self, request, request_type: str):
        """
        Adds to the memory of the node if the key is in the range of the current node.
        If not the request is rerouted to the appropriate node.

        Returns a GetResponse or a PutResponse
        """
        # get coordinator node
        # search for closest node
        # walk up clockwise to find latest node

        key = request.key # assuming this key is in the key space
        print(f"Key is {key}")

        # find token for key
        req_token = key // self.params.Q

        print(f"Token is {req_token}")

        # find node for token
        node = self.token2node[req_token]

        print(f"Node for token {req_token} is {node}")

        # if curr node is not the coordinator
        if self.n_id != node:
            # this request needs to be rerouted to first node in nodes
            print('Rerouting...')
            return self.reroute(node, request_type)

        # if curr node is coordinator node...

        # store it
        self._add_to_hash_table(request)
        print(f"Stored to in memory key val store at node {self.n_id}")

        # send request to all replica nodes
        print("Replicating....")
        response = self.replicate(request)

        # return back to client with 
        return response

    
    def reroute(self, node: int, request_type: str):
        """
        Reroutes the request, gives a rejection signal to the client,
        informs the client on which node to hit.
        node -> node id
        request_type = (get/put)
        """
        assert(request_type == "get" or request_type == "put")

        if request_type == "put":
            return PutResponse(server_id=self.n_id, metadata="needs to reroute !", reroute=True, reroute_server_id=self.view[node])
        else:
            raise NotImplementedError

    def replicate_to(self, rep_n, request) -> ReplicateResponse:
        """
        Send a RPC call to a process telling it to replicate the request in it's memory
        """
        port = self.view[rep_n]
        print(f"port {port}")
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = DynamoInterfaceStub(channel)
            response = stub.Replicate(request)
        print(f"Replicate Response {response}")
        return response


    def replicate(self, request):
        """
        Replication logic for dynamo DB. Assumes current node is the coordinator node.
        """
        print(f"Preference List is {self.preference_list}")

        # TODO: sequential, can be optimized by doing these requests in parallel
        # for replica_n in self.preference_list
        for p in self.preference_list:
            if p != self.n_id:
                # assuming no failures
                response = self.replicate_to(p, request)

                # assume no failures: TODO: fix
                assert response.succ != False

        return PutResponse(server_id=self.n_id, metadata="Replicated", reroute=False, reroute_server_id=-1)