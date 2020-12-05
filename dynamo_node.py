
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict
from dynamo_pb2 import PutResponse, GetResponse, PutRequest, GetRequest, ReplicateResponse, MemResponse
from partitioning import get_preference_list, createtoken2node, find_owner, get_ranges
from structures import Params
from dynamo_pb2_grpc import DynamoInterfaceStub
import grpc
import threading
import concurrent

def replicate_rpc(view, rep_n, request) -> ReplicateResponse:
    """
    Send a RPC call to a process telling it to replicate the request in it's memory
    """
    port = view[rep_n]
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = stub.Replicate(request)
    return response



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
        self.memory_of_node: Dict[int, PutRequest] = {}
        self.memory_of_replicas: Dict[int, PutRequest] = {}


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
        print(f"Put called for {request.key} at {self.n_id}")

        # add to memory
        response: PutResponse = self._add_to_memory(request, request_type="put")
        
        print(f"Put sending a response back {response}")
        return response
    
    def Replicate(self, request: PutRequest , context):
        """
        Replicate Request
        """
        print(f"Replication called for {request.key} at node {self.n_id}")

        # add to memory
        self._add_to_replica_hash_table(request)

        # construct replicate response
        response = ReplicateResponse(server_id=self.n_id, 
                    metadata="Replication successful", 
                    succ=True)
        
        return response
    
    def _add_to_hash_table(self, request):
        """
        Adds request to in memory hash table.
        """
        self.memory_of_node[request.key] = request
    
    def _add_to_replica_hash_table(self, request):
        """
        Adds request to in memory replicated hash table.
        """
        self.memory_of_replicas[request.key] = request

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

        # find token for key
        req_token = key // self.params.Q


        # find node for token
        node = self.token2node[req_token]


        # if curr node is not the coordinator
        if self.n_id != node:
            # this request needs to be rerouted to first node in nodes
            print('Rerouting...')
            return self.reroute(node, request_type)

        # if curr node is coordinator node...

        # store it
        self._add_to_hash_table(request)

        # send request to all replica nodes
        print("Replicating....")
        response = self.replicate(request)
        print(f"Replication done ! {self.memory_of_replicas}")

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


    def replicate(self, request):
        """
        Replication logic for dynamo DB. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        TODO: add async operation
        """
        print(f"Preference List is {self.preference_list}")
        replica_lock = threading.Lock()
        completed_reps = 0


        def rpc_callback(f):
            print(f"Writes done !")

        # TODO: sequential, can be optimized by doing these requests in parallel
        # for replica_n in self.preference_list
        # send RPC's in parallel
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])
        for p in self.preference_list:
            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(replicate_rpc, self.view, p, request)
                fut.add_done_callback(rpc_callback)
                fs.add(fut)
                # assume no failures: TODO: fix

        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.w_timeout)
        
        try:
            w = 1 # already written on original node
            for it in itrs:
                w += 1
                print(f"ITRS: Writes to {w} nodes done !")
                if w >= self.params.W:
                    break
            # TODO: store the futures that have not finished

        except concurrent.futures.TimeoutError:
            # time has expired
            print("Time has expired !")
            # TODO: fail request as put request did not succeed ? What do we do here
            
        return PutResponse(server_id=self.n_id, metadata="Replicated", reroute=False, reroute_server_id=-1)
    
    def PrintMemory(self, request, context):
        """
        Prints current state of the node
        function meant for debugging purposes
        """
        print("-------------------------------------------")
        print(f"Information for {self.n_id}")
        print(f"Preference List: {self.preference_list}")
        print("The memory store for current node:")
        for key, val in self.memory_of_node.items():
            print(f"Key: {key} | Val: {val.val}")
        
        print("The memory store for replicated items:")
        for key, val in self.memory_of_replicas.items():
            print(f"Key: {key} | Original Owner {find_owner(key, self.params, self.token2node)}| Val: {val.val}")
        
        print("The membership information is:")
        for key, val in self.membership_information.items():
            ranges = get_ranges(val, self.params.Q)
            print(f" Node {key} has the following tokens {ranges}")
        
        print("-------------------------------------------")

        response = MemResponse(mem=self.memory_of_node, mem_replicated=self.memory_of_replicas)
        return response
        



