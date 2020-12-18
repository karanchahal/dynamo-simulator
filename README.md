# An Implementation of DynamoDB
This repository seeks to serve as an overview of the key aspects of Dynamo by diving straight into snippets of code.

## Setup
To install all dependencies, run the followign commands
1. ```conda create -n dynamo python=3.6```
2. ```conda activate dynamo```
3. ```pip install -r requirements.txt```
4. ```sh gen_proto.sh``` to generate the protobuf files.

## Run 
Then to start the server, run ```python spawn.py``` and to run the web simulator:
1. ```cd simulator```
2. ```python main.py```

## Main Functionality

- Our implementation uses python multiprocessing to spawn *n* processes and each process communicates with each other using GRPCs.

- The class object that acts like a server for each node is defined in ```dynamo_node.py```. This class object contains the following main public RPC functions:

  1. ```Get()``` -> This function asks the server for the value for a particular key. The ```Read()``` function performs read to form a sloppy quorum.
  2. ```Put()``` -> This function asks the server to add a key value pair to the Dynamo Hash Table instance. Note that the key will not neccesarily be stored in the node for which the original ```Put()``` request is made, it may be rerouted according to the Dynamo protocol. A function ```Replicate()``` replicates a Put request.
  3. Other functions that allow for dynamic failing and gossiping between node are ```Fail()``` (to fail a certain node or bring it back up), ```Transfer()``` (for hinted handoff recovery) and ```Gossip()``` to toggle the gossip protocol behavior.

- The dynamo instance can accessed by client functions mentioned in the file ```client_dynamo.py```

## Tests

We test the folloing functionalities:

1. A test (```test_get_put.py```) that performs typical get and put reqiests and verifies that the nodes have stored these values and replicated the data in the right nodes.
2. A test (```test_failure.py```) that fails certain ndoes and tests get/put behaviour and checks for successful hinted handoff.
3. A test (```test_gossip.py```) that tests the gossip protocol and checks for successful transfer of data to a previosuly failed node & successful deletion of hinted handoff data.
4. A test that checks for replication logic (```test_replication.py```)

## Data Structures and Objects

- The objects transferred over the wire are mentioned in the ```dynamo.proto``` protobuf file. 
- The other objects are mentioned in ```structures.py```.


We have tried to document the code as much as possible to make ti easy to read. Please raise an issue if you think something is unclear and we shall fix it !
