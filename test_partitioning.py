
import partitioning
from structures import Params

def test_init_membership_list():
    num_proc = 4 
    hash_size = 3 # to determine all possible keys in the ring
    Q = 2 # size of 1 virtual node

    membership_info = partitioning.init_membership_list(num_proc, hash_size, Q)

    print(membership_info)

def test_get_preference_list():
    num_proc = 4 
    hash_size = 3 # to determine all possible keys in the ring
    Q = 2 # size of 1 virtual node
    N = 2
    params = Params(dict(num_proc=num_proc, hash_size=hash_size, Q=Q, N=N))

    membership_info = partitioning.init_membership_list(num_proc, hash_size, Q)

    print(membership_info)

    print(0, partitioning.get_preference_list(n_id=0, membership_info=membership_info, params=params))
    print(1, partitioning.get_preference_list(n_id=1, membership_info=membership_info, params=params))
    print(2, partitioning.get_preference_list(n_id=2, membership_info=membership_info, params=params))
    print(3, partitioning.get_preference_list(n_id=3, membership_info=membership_info, params=params))

test_get_preference_list()
