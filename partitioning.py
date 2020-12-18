import numpy as np
from typing import Dict, List, Set, Tuple
from structures import Params

def is_pow_of_two(n: int) -> bool:
    """ 
    Helper function to hceck if num is a power of two.
    """
    print(f"{n} and {(n & (n-1) == 0) and n != 0}")
    return (n & (n-1) == 0) and n != 0


def init_membership_list(params: Params) -> Dict[int, List[int]]: # 2^{12}, 12 bit key hash
    """
    Follows strategy 3 in the Dynamo Paper.

    Returns a dictionary indexing the nodes with membership info
    """

    assert(is_pow_of_two(params.Q) and is_pow_of_two(params.num_proc))

    key_space = pow(2, params.hash_size)
    total_v_nodes = round(key_space / params.Q)
    v_nodes_per_proc = round(total_v_nodes / params.num_proc)

    v_nodes = np.arange(total_v_nodes)

    print(f"Configuration is as follows: Key space sizes {key_space}")
    print(f"Total V Nodes {total_v_nodes} | V Nodes Per Proc {v_nodes_per_proc}")
    # randomly allot `num_v_nodes` to each node
    np.random.shuffle(v_nodes)

    alloc_v_nodes = np.reshape(v_nodes, (params.num_proc, v_nodes_per_proc))
    membership_info = {}
    for i in range(params.num_proc):
        membership_info[i] = alloc_v_nodes[i].tolist()

    return membership_info


def createtoken2node(membership_info: Dict[int, List[int]]) -> Dict[int, int]:
    """
    Creates a dict that has key -> token number and value -> node id
    """
    token2node = {}
    for key, val in membership_info.items():
        for v in val:
            token2node[v] = key

    return token2node


def get_preference_list_skip_unhealthy(n_id: int, membership_info: Dict[int,List[int]], params: Params, unhealthy_nodes: Set[int] = set(), timeout=1):
    '''
    Returns a pref list of size N (`N` specified in params).

    We use Strategy 3 as our consistent hashing paradigm:
    Q -> size of a token
    T -> number of tokens that cover entire key space K
    S - > number of nodes

    For each node, to find it's replicas. We find it's tokens (M = T/S tokens).
    For each of these tokens, we look at (N-1)/M nodes ahead and find the nodes responsible for those tokens.


    Hence, we would have M*(N/M) total nodes to look at. We find out the nodes responsible for those tokens
    and those nodes form our preference list.

    Please note, that we can have less than N number of nodes as multiple tokens might map to the same node.
    Dynamo states that

    "Note that with the use of virtual nodes, it is possible
    that the first N successor positions for a particular key may be
    owned by less than N distinct physical nodes (i.e. a node may
    hold more than one of the first N positions). To address this, the
    preference list for a key is constructed by skipping positions in the
    ring to ensure that the list contains only distinct physical nodes"

    TODO: Add skips so that replicas are distinct nodes

    n_id: node id for which to construct pref list
    membership_info: List of tokens given to each node
    params: Dynamo params
    timeout: 1 second
    '''
    # get positions of each server in the ring

    key_space = pow(2, params.hash_size)
    total_v_nodes = round(key_space / params.Q)
    v_nodes_per_proc = round(total_v_nodes / params.num_proc)
    token2node = createtoken2node(membership_info) # could have multiple nodes for a single token
    pref_list = set([])
    token_list = []

    pref_list.add(n_id)
    # for token in membership_info[n_id]:
    #     i = 1
    #     nodes_added = 0
    #     while True:
    #         replic_token = (token+i) % total_v_nodes
    #         n = token2node[replic_token]
    #         if (n != n_id) and (n not in pref_list) and (n not in unhealthy_nodes):
    #             pref_list.add(n)
    #             token_list.append(replic_token)
    #             nodes_added += 1

    #         if nodes_added == params.num_proc - 1:
    #             break

    #         i += 1

    nodes_added = 0
    for replica_token in range(total_v_nodes):
        n = token2node[replica_token]
        if (n != n_id) and (n not in pref_list) and (n not in unhealthy_nodes):
            pref_list.add(n)
            token_list.append(replica_token)
            nodes_added += 1

        if nodes_added == params.num_proc - 1:
            break

    # Note: this will trigger if we do not have at least N unique replicas
    assert len(pref_list) >= params.N
    return pref_list, token_list


def get_preference_list_for_token(token: int, token2node: Dict[int,int], params: Params, unhealthy_nodes: Set[int] = set(), timeout=1) -> Tuple[List[int], List[int]]:
    """
    Using Strategy 3 of the consistent hashing paradigm, we walk the ring in a clockwise direction and pick the next 'N-1' tokens belonging to a node
    different from the current node. In order to account for failures, we skip over tokens belonging to the unhealthy nodes
    Note: Beware that this can return less than N healthy nodes, if the system isn't healthy.
    """
    key_space = pow(2, params.hash_size)
    total_v_nodes = round(key_space / params.Q)
    curr_nid = token2node[token]

    pref_list = [token]
    node_list = [curr_nid]
    replica_token = token
    replica_token = (replica_token+1) % total_v_nodes
    while replica_token != token:
        n = token2node[replica_token]
        if (n != curr_nid) and (n not in node_list) and (n not in unhealthy_nodes):
            pref_list.append(replica_token)
            node_list.append(n)
        if len(node_list) == params.N: # can be chanced to params.num_proc if we want more than N nodes returned in pref_list
            break
        replica_token = (replica_token+1) % total_v_nodes

    return pref_list, node_list

def find_owner(key: int, params: Params, token2node: Dict[int, int]):
    """
    Find's owner of a certain key dependeing on membership info (incapsulated in token2node)
    Returns node id
    """
    # find token number
    req_token = key // params.Q

    # find node for token
    node = token2node[req_token]

    return node

def get_ranges(tokens: List[int], token_sz: int):
    """
    Returns ranges given the tokens.
    Hence, if token size is 2
    and tokens are 0 and 1.
    then range would be
    0->1,2->3
    tokens: list of token ids
    token_sz: size of each token
    """
    ranges = []
    for t in tokens:
        ranges.append(f"({t*token_sz} -> {(t+1)*token_sz}]")
    return ranges
