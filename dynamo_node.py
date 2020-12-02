
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict
from dynamo_pb2 import PutResponse, GetResponse
from partioning import get_preference_list


class DynamoNode(DynamoInterfaceServicer):
    """
    This class is the entry point to a dynamo node.
    """
    def __init__(self, n_id: int, view: Dict[int, str], 
        membership_information: Dict[int, List[int]], params: Params):
        """
        view: dic indexed by node id, value is the address of the node
        membership_information: dict indexed by node id, gives set of tokens for each node. 
            Each token states the starting point of a virtual node which is of size `Q` (see `Params`).
        params: Stores config information of the dynamo ring
        """
        super().__init__()

        self.n_id = n_id

        # tokens given to various nodes in the ring: bootstrapped information
        self.membership_information = membership_information

        # list of address to other nodes, indexed by node id
        self.view = view

        # a list of > N nodes that are closest to current node, (clockwise)
        self.preference_list = get_preference_list(n_id=n_id, membership_information, params=params)

        # in memory data store of key, values
        self.memory: List[KeyValPair] = []


    def Get(self, request, context):
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


    def Put(self, request, context):
        """
        Put Request
        """
        print(f"put called by client({request.client_id}) for key: {request.key}")

        # add to memory
        self._add_to_memory(request)

        response = PutResponse(
            server_id=self.n_id,
            metadata="Success ?")
        return response
    

    def _add_to_memory(self, request):
        # get coordinator node
        # search for closest node
        # walk up clockwise to find latest node

        key = request.key # assuming this key is in the key space

        # find token
        req_token = key/self.params.Q

        nodes = self.token2node[req_token]

        if self.n_id not in nodes:
            # this request needs to be rerouted to first node in nodes
            print('Rerouting...')
            raise NotImplementedError
            return

        # if curr node is one of the node in `nodes`

        # store it
        self.add_to_hash_table(request)

        # send request to all replica nodes

        raise NotImplementedError