# RAFT

Leader election via the RAFT protocol

## Introduction 

RAFT protocol is a distributed consensus algorithm. Consensus means multiple servers agreeing on same information, something imperative to design fault-tolerant distributed systems. 

## Configuration File

The configuration file (config.txt) indicates (1) how many instances of your server will run, (2) where those instances are running. The config file looks like:

	N: 4
	node0: <host>:<port>
	node1: <host>:<port>
	node2: <host>:<port>
	node3: <host>:<port>

N specifies the number of nodes, which can be up to 9. Then there is a line for each node, starting at 0 and going up to (N-1).

## To Run

	$ python3 raftnode.py ./myconfig.txt 3

There are two arguments: the first is the location of the configuration file, and the second is an indication of which node the program represents.

## References

[RAFT Website](https://raft.github.io/)
[RAFT Visualization](http://thesecretlivesofdata.com/raft/)
[Raft Paper](https://raft.github.io/raft.pdf)