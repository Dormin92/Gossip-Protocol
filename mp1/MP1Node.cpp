/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <string>
#include <random>

using namespace std;

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

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
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
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
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
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
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

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
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
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

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) 
{
    MessageHdr *msg = (MessageHdr*) data;

    if(msg->msgType == JOINREQ)
    {
        HandleJoinRequest(msg);
    }
    else if(msg->msgType == JOINREP)
    {
        HandleJoinReply(msg, data, size);

        // //Printing to debuglog
        // #ifdef DEBUGLOG
        //     log->LOG(&memberNode->addr, "Received Table:");
        //     for(auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++)
        //     {
        //         string s = "ID: " + to_string(it->getid()) + " Heartbeat: " + to_string(it->getheartbeat()) + " Timestamp: " + to_string(it->gettimestamp());
        //         log->LOG(&memberNode->addr, s.c_str());
        //     }
        //     log->LOG(&memberNode->addr, "-----------------------");
        // #endif
    }
    else if(msg->msgType == GOSSIP)
    {
        HandleGossip(msg, data, size);
    }
    return true;
}

void MP1Node::HandleJoinRequest(MessageHdr* msg)
{
    //get address and heartbeat from joinreq
    long *newMemberHeartBeat = new long;
    Address *replyAddr = new Address;
    memcpy(replyAddr            , (char *)(msg+1)                           , sizeof(Address));
    memcpy(newMemberHeartBeat   , (char *)(msg+1) + 1 + sizeof(Address)     , sizeof(long));

    //Add this member to member list
    int id = replyAddr->addr[0];
    short port = replyAddr->addr[4];
    memberNode->memberList.emplace_back(id, port, *newMemberHeartBeat, (long)par->getcurrtime());
    log->logNodeAdd(&memberNode->addr, replyAddr);

    //create JOINREP message
    size_t MemListSize = sizeof(MemberListEntry) * (memberNode->memberList.size()+1); //+1 to make room for self
    size_t msgsize = sizeof(MessageHdr) + MemListSize;
    msg = (MessageHdr *) operator new(msgsize * sizeof(char));
    msg->msgType = JOINREP;
    char serialisedMemberList[MemListSize];
    SerialiseMemberList(serialisedMemberList);
    memcpy((char *)(msg+1), serialisedMemberList, MemListSize);

    //send joinrep
    emulNet->ENsend(&memberNode->addr, replyAddr, (char *)msg, msgsize);
    
    //delete pointers
    delete msg;
    delete newMemberHeartBeat;
    delete replyAddr;
}

void MP1Node::HandleJoinReply(MessageHdr* msg, char* data, int size)
{
    size -= sizeof(MessageHdr);
    data += sizeof(MessageHdr);

    while(size > 0)
    {
        int id = *(int*)(&memberNode->addr.addr);
        MemberListEntry MLE  = DeserialiseMemberListEntry(data);

        //make sure current entry is not self
        if(MLE.getid() != id)
        {
            memberNode->memberList.emplace_back(MLE);
            Address* NewNodeAddress = new Address(to_string(MLE.getid()) + ":"  + to_string(MLE.getport()));
            log->logNodeAdd(&memberNode->addr, NewNodeAddress);
        }

        data += sizeof(MemberListEntry);
        size -= sizeof(MemberListEntry);
    }

    //node is now in group
    memberNode->inGroup = true;

    //update number of neighbors
    memberNode->nnb = memberNode->memberList.size();
}

void MP1Node::HandleGossip(MessageHdr* msg, char* data, int size)
{
    size -= sizeof(MessageHdr);
    data += sizeof(MessageHdr);
    vector<MemberListEntry> ReceivedMemberList;
    while(size > 0)
    {
        int id = *(int*)(&memberNode->addr.addr);
        MemberListEntry MLE  = DeserialiseMemberListEntry(data);

        //make sure current entry is not self
        if(MLE.getid() != id)
            ReceivedMemberList.emplace_back(MLE);

        data += sizeof(MemberListEntry);
        size -= sizeof(MemberListEntry);
    }

    //compare new members with existing ones
    for(auto r_It = ReceivedMemberList.begin(); r_It != ReceivedMemberList.end(); r_It++)
    {
        auto Local_Iterator = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), [r_It](MemberListEntry MLE){return r_It->getid() == MLE.getid(); });
        
        //is entry present in memberlist?
        if(Local_Iterator != memberNode->memberList.end())
        {
            //if present, is it more recent?
            if(Local_Iterator->gettimestamp() < r_It->gettimestamp())
            {
                Local_Iterator->setheartbeat(r_It->getheartbeat());
                Local_Iterator->settimestamp(r_It->gettimestamp());
            }
        }
        else if((par->getcurrtime() - r_It->gettimestamp()) < TFAIL)//if not present, and not marked as failed (through timestamp), then add to list
        {
            memberNode->memberList.emplace_back(r_It->getid(), r_It->getport(), r_It->getheartbeat(), par->getcurrtime());
            Address* NewNodeAddress = new Address(to_string(r_It->getid()) + ":"  + to_string(r_It->getport()));
            log->logNodeAdd(&memberNode->addr, NewNodeAddress);
        }
    }

    //update number of neighbors
    memberNode->nnb = memberNode->memberList.size();
}

void MP1Node::SerialiseMemberList(char* outMemberList)
{
    //serialize self
    char Entry[sizeof(MemberListEntry)];
    int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);
    MemberListEntry self = MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
    SerialiseMemberEntry(self, Entry);
    memcpy(outMemberList, Entry, sizeof(MemberListEntry));

    //serialize memberlist
    for(int i = 0; i < memberNode->memberList.size(); i++)
    {
        char Entry[sizeof(MemberListEntry)];
        SerialiseMemberEntry(memberNode->memberList[i], Entry);
        memcpy(outMemberList + (sizeof(MemberListEntry) * (i + 1)), Entry, sizeof(MemberListEntry));
    }
}

void MP1Node::SerialiseMemberEntry(MemberListEntry m, char* outListEntry)
{
    int id = m.getid();
    short port = m.getport();
    long hb = m.getheartbeat();
    long ts = m.gettimestamp();
    memcpy(outListEntry, &id, sizeof(id));
    memcpy(outListEntry + sizeof(id), &port, sizeof(port));
    memcpy(outListEntry + sizeof(id) + (sizeof(port) * 2), &hb, sizeof(hb));
    memcpy(outListEntry + sizeof(id) + (sizeof(port) * 2) + sizeof(hb), &ts, sizeof(ts));
}

MemberListEntry MP1Node::DeserialiseMemberListEntry(char* data)
{
    MemberListEntry MLE;
    memcpy(&MLE.id          , data                                                                      , sizeof(MLE.id));
    memcpy(&MLE.port        , data + sizeof(MLE.id)                                                     , sizeof(MLE.port));
    memcpy(&MLE.heartbeat   , data + sizeof(MLE.id) + sizeof(MLE.port) + 2                              , sizeof(MLE.heartbeat));
    memcpy(&MLE.timestamp   , data + sizeof(MLE.heartbeat) + sizeof(MLE.id) + sizeof(MLE.port) + 2      , sizeof(MLE.timestamp));
    return MLE;
}

vector<int> MP1Node::RandomNeighbors(int NumberOfNeighbors)
{
    vector<int> v;
    for(int i = 0; i < NumberOfNeighbors; i++)
    {
        v.push_back(i);
    }
    random_device rd;
    mt19937 g(rd());
    shuffle(v.begin(), v.end(), g);
    return v;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() 
{
    //if node is in group
    if(memberNode->inGroup)
    {
        memberNode->heartbeat++;

        int t = par->getcurrtime();
        auto TimeOutIterator = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), [t](MemberListEntry MLE){  return (t - MLE.gettimestamp() > TREMOVE);  });
        if(TimeOutIterator != memberNode->memberList.end())
        {
            Address* TimedOutNodeAddress = new Address(to_string(TimeOutIterator->getid()) + ":"  + to_string(TimeOutIterator->getport()));
            log->logNodeRemove(&memberNode->addr, TimedOutNodeAddress);
            memberNode->memberList.erase(TimeOutIterator);
        }

        //construct messageHdr with type GOSSIP. Initialise size as the size of membership table
        size_t MemListSize = sizeof(MemberListEntry) * (memberNode->memberList.size());
        size_t msgsize = sizeof(MessageHdr) + MemListSize;
        MessageHdr* msg = (MessageHdr *) operator new(msgsize * sizeof(char));
        msg->msgType = GOSSIP;
        
        int LIST_SIZE = memberNode->memberList.size();
        vector<int> rn = RandomNeighbors(memberNode->nnb);

        for(int i = 0; i < memberNode->nnb/2; i++)
        {
            int NeighborToGossipTo = rn[i];
            Address* NeighborAddr = new Address(to_string(memberNode->memberList[NeighborToGossipTo].getid()) + ":"  + to_string(memberNode->memberList[NeighborToGossipTo].getport()));

            //serialise membership list and memcpy it to the messageHdr
            char serialisedMemberList[MemListSize];
            SerialiseMemberList(serialisedMemberList);
            memcpy((char *)(msg+1), serialisedMemberList, MemListSize);

            //ENsend
            emulNet->ENsend(&memberNode->addr, NeighborAddr, (char *)msg, msgsize);

            delete NeighborAddr;
        }

        delete msg;
    }
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
