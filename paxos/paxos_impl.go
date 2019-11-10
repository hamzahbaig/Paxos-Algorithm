package paxos

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	// TODO: implement this!
	myHostPort            string
	ID                    int
	numNodes              int
	majorityNodes         int
	proposalNumber        int
	clients               []*rpc.Client
	proposalNumberKeyPair map[string]int
	myConnection          *rpc.Client
	minProposal           int //previous min Accepted Proposal
}

func (pn *paxosNode) prepareHandler(args *paxosrpc.ProposeArgs, prepareChan chan *paxosrpc.PrepareReply, conn *rpc.Client) {
	preparePacket := &paxosrpc.PrepareArgs{
		Key:         args.Key,
		N:           args.N, //proposal Number
		RequesterId: pn.ID}
	var reply1 paxosrpc.PrepareReply
	err := conn.Call("PaxosNode.RecvPrepare", preparePacket, &reply1)
	if err != nil {
		prepareChan <- nil
	} else {
		prepareChan <- &reply1
	}
}

func (pn *paxosNode) proposerHandler(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply, done chan error) {

	// PHASE 1: Send prepare packets to all Acceptors
	prepareChan := make(chan *paxosrpc.PrepareReply)
	for _, conn := range pn.clients {
		go pn.prepareHandler(args, prepareChan, conn)
	}
	// PHASE 1 REPLY: Handling replty of prepare packets
	for {
		prepareReply := <-prepareChan
		if prepareReply == nil {
			fmt.Println("Working")
			break
		}
		if prepareReply.Status == paxosrpc.OK {
			if prepareReply.N_a == -1 {
				fmt.Println("Working..")
			}
		}
	}
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	pn := &paxosNode{
		// personal info
		myHostPort:            myHostPort,
		ID:                    srvId,
		numNodes:              numNodes,
		majorityNodes:         numNodes/2 + 1,
		proposalNumber:        srvId * 50,
		proposalNumberKeyPair: make(map[string]int),
		minProposal:           -1}
	// clients:               make([]*rpc.Client)}

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Error...")
		return nil, err
	}
	fmt.Println("Server ", srvId, " is listening...")
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(pn))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	// index := 0
	for id, s := range hostMap {
		for i := 1; i <= numRetries; i++ {
			conn, err := rpc.DialHTTP("tcp", s)
			if srvId == id {
				pn.myConnection = conn
				break
			}
			if err != nil {
				time.Sleep(1 * time.Second)
			} else {
				pn.clients = append(pn.clients, conn)
				fmt.Println("Server ", srvId, " made a successfull connection with server", id)
				break
			}
			if i == numRetries {
				return nil, errors.New("Server Dead: " + s)
			}
		}
		// index++
	}
	return pn, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key

func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	pn.proposalNumberKeyPair[args.Key] = pn.proposalNumber
	reply.N = pn.proposalNumberKeyPair[args.Key]
	pn.proposalNumber++
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {

	fmt.Println(pn.ID)
	done := make(chan error)
	pn.proposerHandler(args, reply, done)
	return nil
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	// never accepted
	if pn.minProposal == -1 {
		reply.Status = paxosrpc.OK
		reply.N_a = -1
		reply.V_a = nil
	}
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	return errors.New("not implemented")
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	return errors.New("not implemented")
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	return errors.New("not implemented")
}
