/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

class Bundle {
public:
	int success_responses;
	int failure_responses;
	MessageType t;
	string key;
	string value;
	int iteration;

	Bundle(MessageType t, string key, string value) {
		this->success_responses = 0;
		this->failure_responses = 0;
		this->t = t;
		this->key = key;
		this->value = value;
		this->iteration = 0; // the times being read by checkMessages
	}
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	// for key-value of key-value store
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// new ring
	vector<Node> new_ring;
	// for tracking transactions
	int lowestTransID;
	// record the number of replies of a transaction
	map<int, Bundle*> transID2Bundle; // don't have to initialize

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);
	vector<Node> findNodes2(string key, vector<Node> ring);

	// server
	bool createKeyValue(Address fromAddr, string key, string value, ReplicaType replica, int transID, MessageType t);
	string readKey(Address fromAddr, string key, int transID, MessageType t);
	bool updateKeyValue(Address fromAddr, string key, string value, ReplicaType replica, int transID, MessageType t);
	bool deletekey(Address fromAddr, string key, int transID, MessageType t);
	
	void handleReply(int transID, bool success);
	void handleReadReply(string value, int transID);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	void clientReadDelete(string key, MessageType t);
	void clientCreateUpdate(string key, string value, MessageType t);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
