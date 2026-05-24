package raft

import (
	"fmt"
	"log"
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

	go n.startReporting()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Node %d] accept error: %v", n.ID, err)
			continue
		}
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
	fmt.Printf("Node %d received ping: %s\n", rpc.node.ID, args.Message)
	reply.Message = "pong"
	return nil
}

func (rpc *RpcHandler) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	node := rpc.node
	node.mu.Lock()
	defer node.mu.Unlock()

	reply.Term = node.CurrentTerm
	reply.VoteGranted = false

	if args.Term < node.CurrentTerm {
		log.Printf("[Node %d] rejected vote for Node %d — stale term (%d < %d)", node.ID, args.CandidateID, args.Term, node.CurrentTerm)
		return nil
	}

	if args.Term > node.CurrentTerm {
		node.becomeFollower(args.Term)
		reply.Term = node.CurrentTerm
	}

	if node.VotedFor == -1 || node.VotedFor == args.CandidateID {
		node.VotedFor = args.CandidateID
		reply.VoteGranted = true
		log.Printf("[Node %d] granted vote to Node %d (term=%d)", node.ID, args.CandidateID, args.Term)
	} else {
		log.Printf("[Node %d] denied vote to Node %d — already voted for Node %d (term=%d)", node.ID, args.CandidateID, node.VotedFor, args.Term)
	}

	reply.Term = node.CurrentTerm

	return nil
}

func (rpc *RpcHandler) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	node := rpc.node
	node.mu.Lock()
	defer node.mu.Unlock()

	reply.Term = node.CurrentTerm
	reply.Success = false

	if args.Term < node.CurrentTerm {
		log.Printf("[Node %d] rejected heartbeat from Node %d — stale term (%d < %d)", node.ID, args.LeaderID, args.Term, node.CurrentTerm)
		return nil
	}

	if args.Term > node.CurrentTerm || node.State != Follower {
		node.becomeFollower(args.Term)
	}

	reply.Term = node.CurrentTerm
	reply.Success = true

	select {
	case node.heartbeatCh <- struct{}{}:
	default:
	}

	return nil
}
