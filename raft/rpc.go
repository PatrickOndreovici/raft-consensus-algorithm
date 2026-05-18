package raft

import (
	"fmt"
	"net"
	"net/rpc"
)

type RpcHandler struct {
	node *Node
}

func NewRpcHandler(node *Node) *RpcHandler {
	return &RpcHandler{node: node}
}

func (n *Node) Start() {

	server := rpc.NewServer()

	handler := NewRpcHandler(n)

	server.Register(handler)

	listener, err := net.Listen("tcp", n.Addr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node %d listening on %s\n", n.ID, n.Addr)

	go n.run()

	for {
		conn, _ := listener.Accept()
		go server.ServeConn(conn)
	}
}

type PingArgs struct {
	Message string
}

type PingReply struct {
	Message string
}

func (rpc *RpcHandler) Ping(args PingArgs, reply *PingReply) error {
	fmt.Printf(
		"Node %d received ping: %s\n",
		rpc.node.ID,
		args.Message,
	)

	reply.Message = "pong"

	return nil
}

func (rpc *RpcHandler) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rpc.node.mu.Lock()
	defer rpc.node.mu.Unlock()

	node := rpc.node

	reply.Term = node.CurrentTerm
	reply.VoteGranted = false

	if args.Term < node.CurrentTerm {
		return
	}

	if args.Term > node.CurrentTerm {
		node.CurrentTerm = args.Term
		node.State = Follower
		node.VotedFor = -1
		reply.Term = node.CurrentTerm
	}

	if node.VotedFor == -1 || node.VotedFor == args.CandidateID {
		node.VotedFor = args.CandidateID
		reply.VoteGranted = true
	}

	reply.Term = node.CurrentTerm
}

func (rpc *RpcHandler) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

}
