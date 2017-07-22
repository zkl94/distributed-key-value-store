/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: THIS IS GOSSIP STYLE MEMBERSHIP
 * LINK: https://www.cs.cornell.edu/home/rvr/papers/GossipFD.pdf
 **********************************/

#include "MP1Node.h"
#include <cassert>
#include <cstdlib>

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
    this->broadcast = 0;
    this->tobedeleted = new std::map<int, int>;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *              This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *              All initializations routines for a member.
 *              Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    // join address is the address of the introducer
    Address joinaddr;
    joinaddr = getJoinAddress();

    Address *my_addr = &this->memberNode->addr;

    // Self booting routines
    if( initThisNode(my_addr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *my_addr) {
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {

        // member node in group only after receiving JOINREP

        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    // add self to memberlist; self is ALWAYS the first member
    int id = 0;
    short port;
    memcpy(&id, &(memberNode->addr.addr[0]), sizeof(int));
    memcpy(&port, &(memberNode->addr.addr[4]), sizeof(short));
    MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    emulNet->ENcleanup();
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *              Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

void MP1Node::logNodeAddWrapper(Member *memberNode, int id, short port) {
    Address address;
    string _address = to_string(id) + ":" + to_string(port);
    address = Address(_address);
    log->logNodeAdd(&memberNode->addr, &address);
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {

    // for introduction:
    // two message types: JOINREQ, JOINREP
    // 1. when introducer receives JOINREQ, add the node to member list, and return JOINREP with cluster member list
    // 2. when cluster member receives JOINREP, add the cluster member list

    // for membership:
    // one message type: MEMBERLIST
    // 1. when receive MEMBERLIST, update you own member list accordingly

    MsgTypes msgType = ((MessageHdr *)data)->msgType;

    switch (msgType) {
        case JOINREQ:
            handleJOINREQ(env, data, size);
            break;
        case JOINREP:
            handleJOINREP(env, data, size);
            break;
        case MEMBERLIST:
            handleMEMBERLIST(env, data, size);
            break;
        default:
            cout << "unknown message type" <<endl;
            break;
    }

    return false;
}

// #define DEBUG_JOINREQ

void MP1Node::handleJOINREQ(void *env, char *data, int size) {
    #ifdef DEBUG_JOINREQ
        cout << "received a join request" << endl;
    #endif

    // when introducer receives JOINREQ, add the node to member list, and return JOINREP with cluster member list
    char addr[6];
    // msg: sizeof(MessageHdr) + 6 + sizeof(long)
    MessageHdr *msg = (MessageHdr *)data;
    memcpy(&addr, (char *)(msg)+4, 6);
    int id = 0;
    short port;
    memcpy(&id, &addr[0], sizeof(int));
    memcpy(&port, &addr[4], sizeof(short));

    #ifdef DEBUG_JOINREQ
        cout << "id = " << id << "; port = " << port << endl;
    #endif

    #ifdef DEBUG_JOINREQ
        cout << "before adding member list entry, there are " << memberNode->memberList.size() << " entries" << endl;
    #endif

    // newly added node have 0 heartbeat
    MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);

    logNodeAddWrapper(memberNode, id, port);

    #ifdef DEBUG_JOINREQ
        cout << "after adding member list entry, there are " << memberNode->memberList.size() << " entries" << endl;
    #endif

    // return JOINREP with cluster member list
    int entry_num = memberNode->memberList.size();
    int currentOffset = 0;
    // allocate maximum space possibly needed
    size_t JOINREPMsgSize = sizeof(MessageHdr) + (sizeof(addr) + sizeof(long)) * entry_num;
    MessageHdr *JOINREPMsg;
    JOINREPMsg = (MessageHdr *) malloc(JOINREPMsgSize * sizeof(char));
    JOINREPMsg->msgType = JOINREP;
    // for the message type offset
    currentOffset += sizeof(int);

    for (MemberListEntry memberListEntry: memberNode->memberList) {
        // if it is to be deleted, don't add it to member list messages
        if (this->tobedeleted->count(memberListEntry.getid()) != 0) {
            continue;
        }

        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        // add to JOINREPMsg
        memcpy((char *)(JOINREPMsg) + currentOffset, &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(JOINREPMsg) + currentOffset, &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
        // only send heartbeat, timestamp is set by receivers with par->getcurrtime()
        // also, timestamp is not global and thus no need to sync
    }

    #ifdef DEBUG_JOINREQ
        cout << "currentOffset = " << currentOffset << " JOINREPMsgSize = " << JOINREPMsgSize << endl;
    #endif

    // not necessarily because to be deleted entries are not included
    // assert(currentOffset == (int)JOINREPMsgSize);

    string _address = to_string(id) + ":" + to_string(port);
    Address address = Address(_address);
    emulNet->ENsend(&memberNode->addr, &address, (char *)JOINREPMsg, currentOffset);
    #ifdef DEBUG_JOINREQ
        cout << "JOINREP successfully sent" <<endl;
    #endif
}

// #define DEBUG_JOINREQ

void MP1Node::handleJOINREP(void *env, char *data, int size) {
    #ifdef DEBUG_JOINREQ
        cout << "just got a join response with size " << size << endl;
    #endif

    handleMEMBERLIST(env, data, size);
    memberNode->inGroup = true;
}

void MP1Node::updateEntry(Member *memberNode, int id, short port, long heartbeat) {
    // if this node (with id) is already in to be deleted map, just ignore it
    auto alreadyintobedeletedlist = this->tobedeleted->count(id);
    if (alreadyintobedeletedlist != 0) {
        // already in to be deleted map
        // cout << "already in tobedeleted map, ignore the update" <<endl;
        return;
    }

    // if it is not to be deleted, then update/insert normally
    bool found = false;
    for (MemberListEntry memberListEntry: memberNode->memberList) {
        if (memberListEntry.getid() == id && memberListEntry.getport() == port) {
            found = true;
            // the incoming heartbeat is more latest
            if (memberListEntry.getheartbeat() < heartbeat) {
                // cout << "doing heartbeat update for " << id <<endl;
                memberListEntry.setheartbeat(heartbeat);
                memberListEntry.settimestamp(par->getcurrtime());
            }
            break;
        }
    }

    if (!found) {
        MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(*newEntry);

        logNodeAddWrapper(memberNode, id, port);
    }
}

// #define DEBUG_MEMBERLIST

void MP1Node::handleMEMBERLIST(void *env, char *data, int size) {
    #ifdef DEBUG_MEMBERLIST
        cout << "just got a member list" << endl;
    #endif

    int entry_num = (size - 4)/ (6+8);
    int currentOffset = 4;
    char addr[6];
    int id = 0;
    short port;
    long heartbeat = 0;

    MessageHdr *msg = (MessageHdr *)data;

    // if, during the process of to be deleted, a heartbeat from that node is received,
    // the node entry should be removed from to be deleted map and member list entry
    // should be re-activated
    // how to tell: the first entry in message is the node's source
    memcpy(&addr, (char *)(msg)+currentOffset, 6);
    memcpy(&id, &addr[0], sizeof(int));
    if (this->tobedeleted->count(id) != 0) {
        std::map<int, int>::iterator it = this->tobedeleted->begin();
    
        while(it != this->tobedeleted->end()) {
            if (it->first == id) {
                this->tobedeleted->erase(it);
                break;
            }
            ++it;
        }
    } // then updateEntry will do the normal update

    // for every addr and heartbeat in the incoming member list
    for (int i = 0; i < entry_num; ++i)
    {
        // extract id and port
        memcpy(&addr, (char *)(msg)+currentOffset, 6);
        currentOffset += 6;
        memcpy(&id, &addr[0], sizeof(int));
        memcpy(&port, &addr[4], sizeof(short));
        
        // heartbeat
        memcpy(&heartbeat, (char *)(msg)+currentOffset, 8);
        currentOffset += 8;

        updateEntry(memberNode, id, port, heartbeat);
    }
}

// check if the timestamp is outdated
bool isEntryInvalid(Member *memberNode, MemberListEntry& memberListEntry) {
    long entry_timestamp = memberListEntry.gettimestamp();
    long node_timestamp = memberNode->memberList[0].gettimestamp();

    if (node_timestamp - entry_timestamp > TFAIL) {
        return true;
    }
    return false;
}

void MP1Node::updateToBeDeletedMap() {

    std::map<int, int>::iterator it = this->tobedeleted->begin();
    
    while(it != this->tobedeleted->end()) {
        it->second = it->second + 1;
        if (it->second >= TREMOVE) {
            // log the removal
            Address address;
            string _address = to_string(it->first) + ":" + to_string(0);
            address = Address(_address);
            log->logNodeRemove(&memberNode->addr, &address);

            // remove from member list
            bool found = false;
            memberNode->myPos = memberNode->memberList.begin();
            while (memberNode->myPos != memberNode->memberList.end()) {
                if ((*memberNode->myPos).getid() == it->first) {
                    // found it
                    found = true;
                    memberNode->myPos = memberNode->memberList.erase(memberNode->myPos);
                    break;
                }
                ++memberNode->myPos;
            }
            if (!found) {
                cout << "the should-be-deleted entry is not found in member list; sth goes wrong" << endl;
            }

            // remove from to-be-deleted map; can be re-added by membership protocol now
            it = this->tobedeleted->erase(it);
        } else {
            ++it;
        }
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *              the nodes
 *              Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // 0. update self heartbeat and tiemstamp
    // 1. Check if any node hasn't responded within a timeout period and then delete the nodes
    // 2. Propagate your membership list to every other member (MEMBERLIST)

    // update self heartbeat
    memberNode->heartbeat++;

    // update the entry of self in self memberlist
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
    memberNode->memberList[0].settimestamp(par->getcurrtime());

    memberNode->myPos = memberNode->memberList.begin();
    while (memberNode->myPos != memberNode->memberList.end()) {
        if(isEntryInvalid(memberNode, *memberNode->myPos)) {
            int id = memberNode->myPos->getid();
            short port = memberNode->myPos->getport();
            Address address;
            string _address = to_string(id) + ":" + to_string(port);
            address = Address(_address);

            // cout << "found invalid entry with id " << id <<endl;

            if (this->tobedeleted->count(id) == 0) {
                // may have already added to tobedeleted map
                this->tobedeleted->insert(std::make_pair(id, 0));
            }

            // can't remove it at once although we are using tobedeleted map
            // because we still have to send broadcast to it
            // memberNode->myPos = memberNode->memberList.erase(memberNode->myPos);
        }
        ++memberNode->myPos;
    }

    this->updateToBeDeletedMap();

    // construct MEMBERLIST message
    int entry_num = memberNode->memberList.size();
    int currentOffset = 0; 
    // allocate maximum space possibly needed
    size_t memberListMsgSize = sizeof(MessageHdr) + (6 + sizeof(long)) * entry_num;
    MessageHdr *memberListMsg;
    memberListMsg = (MessageHdr *) malloc(memberListMsgSize * sizeof(char));
    memberListMsg->msgType = MEMBERLIST;
    currentOffset += sizeof(int);

    // send / construct two kinds of messages
    for (MemberListEntry memberListEntry: memberNode->memberList) {
        // if it is to be deleted, don't add it to member list messages
        if (this->tobedeleted->count(memberListEntry.getid()) != 0) {
            continue;
        }

        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);

        // add to MEMBERLIST
        memcpy((char *)(memberListMsg) + currentOffset, &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(memberListMsg) + currentOffset, &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
    }


    // Propagate your membership list; it is sent to even nodes (entries) to be deleted
    broadcast++;
    // decide whether broadcast or unicast
    if (broadcast >= BROADCAST_INTERVAL) {
        // broadcast
        for (MemberListEntry memberListEntry: memberNode->memberList) {
            Address address;
            string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
            address = Address(_address);
            emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg, currentOffset);
        }
        broadcast = 0;
    } else {
        // unicast
        int member_num = memberNode->memberList.size();
        int index = rand() % member_num;
        Address address;
        string _address = to_string(memberNode->memberList.at(index).getid()) + ":" + to_string(memberNode->memberList.at(index).getport());
        address = Address(_address);
        emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg, currentOffset);
    }

    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "sent member list message");
    #endif

    free(memberListMsg);

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

// 1.0.0.0:0
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
