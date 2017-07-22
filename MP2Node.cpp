/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->lowestTransID = 0;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	// bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	// if stabilization protocol needs to be run, cache the list (now the old list and new list are both present)
	// and then let the stabilization protocol do the work

	// for testing purpose
	this->ring = curMemList;

	// first compare the size
	if (this->ring.size() != curMemList.size()) {
		this->newRing = curMemList;
		this->stabilizationProtocol();
		return;
	}

	// compare every node hash
	for (int i = 0; i < this->ring.size(); ++i)
	{
		if (this->ring[i].nodeHashCode != curMemList[i].nodeHashCode) {
			this->newRing = curMemList;
			this->stabilizationProtocol();
			return;
		}
	}

	// it doesn't change
	return;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

// client functions are executed on coordinators
// coordinators may send to itself

void MP2Node::clientReadDelete(string key, MessageType t) {
	// construct message
	int transID = this->lowestTransID++;
	this->transID2Bundle.emplace(transID, new Bundle(0, t, key, ""));

	Message *primary_message = new Message(transID, this->memberNode->addr, t, key);
	Message *secondary_message = new Message(transID, this->memberNode->addr, t, key);
	Message *tertiary_message = new Message(transID, this->memberNode->addr, t, key);

	// find primary replica position
	// size_t position = this->hashFunction(key);

	vector<Node> v = findNodes(key);

	// Address _toAddr = this->ring[position].nodeAddress;
	string primary_string = primary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[0].nodeAddress), (char *)primary_string.c_str(), primary_string.size());

	// _toAddr = this->ring[position+1].nodeAddress;
	string secondary_string = secondary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[1].nodeAddress), (char *)secondary_string.c_str(), secondary_string.size());

	// _toAddr = this->ring[position+2].nodeAddress;
	string tertiary_string = tertiary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[2].nodeAddress), (char *)tertiary_string.c_str(), tertiary_string.size());

	delete primary_message;
	delete secondary_message;
	delete tertiary_message;

	// these messages go to the three replicas and the coordinator waits for responses
}

void MP2Node::clientCreateUpdate(string key, string value, MessageType t) {
	// construct message
	int transID = this->lowestTransID++;
	this->transID2Bundle.emplace(transID, new Bundle(0, t, key, value));

	Message *primary_message = new Message(transID, this->memberNode->addr, t, key, value, PRIMARY);
	Message *secondary_message = new Message(transID, this->memberNode->addr, t, key, value, SECONDARY);
	Message *tertiary_message = new Message(transID, this->memberNode->addr, t, key, value, TERTIARY);

	// find primary replica position
	// size_t position = this->hashFunction(key);

	vector<Node> v = findNodes(key);

	// Address _toAddr = this->ring[position].nodeAddress;
	string primary_string = primary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[0].nodeAddress), (char *)primary_string.c_str(), primary_string.size());

	// _toAddr = this->ring[position+1].nodeAddress;
	string secondary_string = secondary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[1].nodeAddress), (char *)secondary_string.c_str(), secondary_string.size());

	// _toAddr = this->ring[position+2].nodeAddress;
	string tertiary_string = tertiary_message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &(v[2].nodeAddress), (char *)tertiary_string.c_str(), tertiary_string.size());

	delete primary_message;
	delete secondary_message;
	delete tertiary_message;

	// these messages go to the three replicas and the coordinator waits for responses
}


/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
// called by application (client)
// the coordinator is also a node, chosen randomly by the client
void MP2Node::clientCreate(string key, string value) {
	this->clientCreateUpdate(key, value, CREATE);
}


/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	this->clientReadDelete(key, READ);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	this->clientCreateUpdate(key, value, UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	this->clientReadDelete(key, DELETE);
}


void MP2Node::handleReply(int transID) {
	transID2Bundle[transID]->responses++;
}

void MP2Node::handleReadReply(string value, int transID) {
	transID2Bundle[transID]->responses++;
	transID2Bundle[transID]->value = value;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(Address fromAddr, string key, string value, ReplicaType replica, int transID, MessageType t) {
	// Insert key, value, replicaType into the hash table
	// the transID is used for logging and response
	this->ht->create(key, value);
	// create must will succeed
	log->logCreateSuccess(&(this->memberNode->addr), false, transID, key, value);

	// reply
	Message *message = new Message(transID, fromAddr, t, true);
	string _string = message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &fromAddr, (char *)_string.c_str(), _string.size());

	return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(Address fromAddr, string key, int transID, MessageType t) {
	// Read key from local hash table and return value
	string result = this->ht->read(key);
	Message *message;
	if (!result.empty()) {
		// success
		log->logReadSuccess(&(this->memberNode->addr), false, transID, key, result);
		message = new Message(transID, fromAddr, t, true);
	} else {
		log->logReadFail(&(this->memberNode->addr), false, transID, key);
		message = new Message(transID, fromAddr, t, false);
	}

	string _string = message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &fromAddr, (char *)_string.c_str(), _string.size());

	return result;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(Address fromAddr, string key, string value, ReplicaType replica, int transID, MessageType t) {
	// Update key in local hash table and return true or false
	bool success = this->ht->update(key, value);
	Message *message;
	if (success) {
		log->logUpdateSuccess(&(this->memberNode->addr), false, transID, key, value);
		message = new Message(transID, fromAddr, t, true);
	} else {
		log->logUpdateFail(&(this->memberNode->addr), false, transID, key, value);
		message = new Message(transID, fromAddr, t, false);
	}

	string _string = message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &fromAddr, (char *)_string.c_str(), _string.size());

	return success;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(Address fromAddr, string key, int transID, MessageType t) {
	// Delete the key from the local hash table
	bool success = this->ht->deleteKey(key);
	Message *message;
	if (success) {
		log->logDeleteSuccess(&(this->memberNode->addr), false, transID, key);
		message = new Message(transID, fromAddr, t, true);
	} else {
		log->logDeleteFail(&(this->memberNode->addr), false, transID, key);
		message = new Message(transID, fromAddr, t, false);
	}

	string _string = message->toString();
	emulNet->ENsend(&(this->memberNode->addr), &fromAddr, (char *)_string.c_str(), _string.size());

	return success;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

		Message *msg = new Message(message);
		int transID = msg->transID;
		ReplicaType replica = msg->replica;
		string key = msg->key;
		string value = msg->value;
		Address fromAddr = msg->fromAddr;
		MessageType t = msg->type;

		// MessageType {CREATE, READ, UPDATE, DELETE, REPLY, READREPLY};
		switch(msg->type) {
			case CREATE:
				createKeyValue(fromAddr, key, value, replica, transID, t);
				break;
			case READ:
				readKey(fromAddr, key, transID, t);
				break;
			case UPDATE:
				updateKeyValue(fromAddr, key, value, replica, transID, t);
				break;
			case DELETE:
				deletekey(fromAddr, key, transID, t);
				break;
			case REPLY:
				handleReply(transID);
				break;
			case READREPLY:
				// handleReadReply(transID);
				handleReadReply(value, transID);
				break;
		}
	}

	// @TODO: how long do we have to wait for replies???
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

	std::map<int, Bundle*>::iterator it = this->transID2Bundle.begin();
    
    while(it != this->transID2Bundle.end()) {
    	// 2 (out of 3) is quorum
        if (it->second->responses >= 2) {
            int transID = it->first;
            MessageType t = it->second->t;
            string key = it->second->key;
            string value = it->second->value;

			if (t == READ) {
				// read
				// @TODO: need to aggregate read return values, can't just use handleReply
				log->logReadSuccess(&(this->memberNode->addr), true, transID, key, value);
			} else if (t == UPDATE) {
				log->logUpdateSuccess(&(this->memberNode->addr), true, transID, key, value);
			} else if (t == CREATE) {
				log->logCreateSuccess(&(this->memberNode->addr), true, transID, key, value);
			} else if (t == DELETE) {
				log->logDeleteSuccess(&(this->memberNode->addr), true, transID, key);
			}

			delete it->second;
            it = this->transID2Bundle.erase(it);
        } else {
            ++it;
        }
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}


/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}
