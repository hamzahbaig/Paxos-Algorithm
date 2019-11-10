package paxos

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type acceptedValue struct {
	proposalNumber int
	value          interface{}
}
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
	kvDb                  map[string]interface{}
	acceptedValuesLog     map[string]*acceptedValue
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

func (pn *paxosNode) acceptHandler(key string, n int, v interface{}, conn *rpc.Client, acceptChan chan *paxosrpc.AcceptReply) {
	acceptPacket := &paxosrpc.AcceptArgs{
		Key:         key,
		N:           n,
		V:           v,
		RequesterId: pn.ID}
	var reply1 paxosrpc.AcceptReply
	err := conn.Call("PaxosNode.RecvAccept", acceptPacket, &reply1)
	if err != nil {
		acceptChan <- nil
	} else {
		acceptChan <- &reply1
	}

}
func (pn *paxosNode) commitHandler(key string, v interface{}, conn *rpc.Client, commitChan chan *paxosrpc.CommitReply) {
	commitPacket := &paxosrpc.CommitArgs{
		Key:         key,
		V:           v,
		RequesterId: pn.ID}
	var reply1 paxosrpc.CommitReply
	err := conn.Call("PaxosNode.RecvCommit", commitPacket, &reply1)
	if err != nil {
		commitChan <- nil
	} else {
		commitChan <- &reply1
	}

}

func (pn *paxosNode) proposerHandler(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply, done chan error) {
	key := args.Key
	proposalNumber := args.N
	value := args.V

	//========== P H A S E 1 ===============================================================================//

	// PHASE 1: Brodcasting Prepare Packet//
	prepareChan := make(chan *paxosrpc.PrepareReply)
	for _, conn := range pn.clients {
		go pn.prepareHandler(args, prepareChan, conn)
	}
	promise := 0
	currentHighestNumber := -1

	// PHASE 1 REPLY: Handling reply of prepare packets
	for {
		prepareReply := <-prepareChan
		if prepareReply == nil {
			done <- errors.New("PrepareHandler threw an error.")
			return
		}
		if prepareReply.Status == paxosrpc.Reject {
			done <- errors.New("Prepare Packet Rejected")
			return
		} else {
			promise++
			if prepareReply.N_a > currentHighestNumber && prepareReply.V_a != nil {
				value = prepareReply.V_a
				currentHighestNumber = prepareReply.N_a
			}

			if promise >= pn.majorityNodes {
				break
			}

		}
	}
	//======================================================================================================//

	//========== P H A S E 2 ===============================================================================//

	//PHASE 2: Broadcasting Accept Packet..
	acceptChan := make(chan *paxosrpc.AcceptReply)
	for _, conn := range pn.clients {
		go pn.acceptHandler(key, proposalNumber, value, conn, acceptChan)
	}

	// PHASE 2 REPLY: Handling reply of accept packets
	accept := 0
	for {
		acceptReply := <-acceptChan
		if acceptChan == nil {
			done <- errors.New("Accept error")
			return
		} else if acceptReply.Status == paxosrpc.Reject {
			done <- errors.New("Accept Packet Rejected")
			return
		} else {
			accept++
			if accept >= pn.majorityNodes {
				break
			}
		}
	}
	//======================================================================================================//

	//========== P H A S E 3 ===============================================================================//

	//PHASE 3: Broadcasting Commit Packet..
	commitChan := make(chan *paxosrpc.CommitReply)
	for _, conn := range pn.clients {
		go pn.commitHandler(key, value, conn, commitChan)
	}

	// PHASE 3 REPLY: Handling reply of accept packets
	commit := 0
	for {
		commitReply := <-commitChan
		if commitReply == nil {
			done <- errors.New("Commit Packet Error")
			return
		} else {
			commit++
			if commit >= pn.numNodes {
				reply.V = value
				done <- nil
				return
			}
		}
	}
	//========================================================================================================//
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
		kvDb:              make(map[string]interface{}),
		minProposal:       -1,
		acceptedValuesLog: make(map[string]*acceptedValue)}

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		// fmt.Println("Error...")
		return nil, err
	}
	// fmt.Println("Server ", srvId, " is listening...")
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(pn))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	for id, s := range hostMap {
		for i := 1; i <= numRetries; i++ {
			conn, err := rpc.DialHTTP("tcp", s)
			if srvId == id {
				pn.myConnection = conn
			}
			if err != nil {
				time.Sleep(1 * time.Second)
			} else {
				pn.clients = append(pn.clients, conn)
				// fmt.Println("Server ", srvId, " made a successfull connection with server", id)
				break
			}
			if i == numRetries {
				return nil, errors.New("Server Dead: " + s)
			}
		}
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

	// fmt.Println("Proposing ID: ", pn.ID)
	done := make(chan error, 1)
	go pn.proposerHandler(args, reply, done)
	select {
	case err := <-done:
		return err
	case <-time.After(PROPOSE_TIMEOUT):
		return errors.New("TimeOut Error")
	}
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	value, flag := pn.kvDb[args.Key]
	// fmt.Println("getValue", args.Key, pn.ID, value)
	if flag {
		reply.Status = paxosrpc.KeyFound
		reply.V = value
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
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
	// Case 1: Never Accepted
	if pn.minProposal == -1 {
		reply.Status = paxosrpc.OK
		reply.N_a = -1
		reply.V_a = nil
		return nil
	}
	// Case 2: Already Accepted
	av, exist := pn.acceptedValuesLog[args.Key]
	if exist {
		if args.N > av.proposalNumber {
			reply.N_a = av.proposalNumber
			reply.V_a = av.value
			reply.Status = paxosrpc.OK
		} else {
			reply.Status = paxosrpc.Reject
		}
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
	// Case 1: No value is accepted
	if pn.minProposal == -1 {
		pn.acceptedValuesLog[args.Key] = &acceptedValue{
			proposalNumber: args.N,
			value:          args.V}
		reply.Status = paxosrpc.OK
		return nil
	}

	// Case 2: Already Accepted Value
	av, exist := pn.acceptedValuesLog[args.Key]
	if exist {
		if args.N >= av.proposalNumber {
			pn.acceptedValuesLog[args.Key] = &acceptedValue{
				proposalNumber: args.N,
				value:          args.V}
			reply.Status = paxosrpc.OK
		} else {
			reply.Status = paxosrpc.Reject
		}
	}
	return nil
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
	// fmt.Println("Server: ", pn.ID, " Writing a value.", args.Key, args.V)
	key := args.Key
	value := args.V
	pn.kvDb[key] = value
	return nil
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
