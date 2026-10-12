# MPComm - Multiparty Communication
Very simple demo of multicast communication without coordination. The intent is to demonstrate how things can go wrong as we scale a distributed system (in this case, scaling in the geographical dimension) without taking appropriate measures to coordinate the behavior of its components. Later, this example will serve as the basis to implement coordination protocols, notably for message ordering, as part of course assignments.

## Overall structure
A set of **peer processes** is established and each process multicasts a sequence of messages to all the others at random intervals. Messages are stamped with the ID of the sending process and a local sequence number defined by the sending process. This is a simple attempt to demonstrate the problem of message ordering (or, indeed, the lack of message ordering).

The peer processes run the *peerCommunicatorUDP.py* program, which has two separate threads, one for sending and the other for receiving messages. After starting, a peer processes waits for a signal (from a process that coordinates the experiment) in order to actually start multicasting the sequence of messages. Each peer collects all the received messages (in the order that they were received) and sends the list of messages to the coordinator process, which will compare the order of messages as received by all the peers (as described next).

A separate process runs *comparisonServer.py*, which, besides coordinating the start of the peer processes (by sending the start signal mentioned above), waits for the list of received messages from each peer and compares their order. It reports a metric that indicates the occurrence of out-of-order messages (actually, the number of message exchange rounds in which at least one process received a different message form the others). Importantly, we assume that no messages are lost, and, in an ideal scenario, all processes should receive all the same messages in the same order. Note, however, that the current version does not use a reliable message communication protocol to ensure that messages are not lost (this can be an improvement to be implemented as an assignment).

A second server process, running *GroupMngr.py*, is used for peers to register themselves (notably their IP addresses) as well as to discover the other peers.

In order to actually see the problem of message ordering, it is necessary to introduce variability on message communication latency, such as by running the peer processes on different networks, e.g., running some of the peers in one region of the cloud, whereas the others are run on another region. This illustrates the issue of geographic scalability, when the system works fine when all its components are in the same network, but starts to behave erroneously when its components are spread across the Internet.

## Setup
- Create one instance (virtual machine in the cloud) to run the two servers, and a number of instances (e.g., 6 in one cloud region, 2 on another) to run the peers.
- Edit *constMP.py* with the correct IP address of the instance where the two servers will run. Tip: allocate a fixed IP address (elastic IP in AWS jargon) to the server instance. Also edit the file to configure the number of peers processes in the experiment.
- On the server instance, run *GroupMngr.py*
- On some of the peer instances: run *peerCommunicator.py*
- Back on the server instance: run *comparisonServer.py*
