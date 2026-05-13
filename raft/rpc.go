package raft

import (
	"fmt"
	"net"
	"net/rpc"
)

func (n *Node) Start() {

	server := rpc.NewServer()

	server.Register(n)

	listener, err := net.Listen("tcp", n.Addr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node %d listening on %s\n", n.ID, n.Addr)

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

func (n *Node) Ping(args PingArgs, reply *PingReply) error {
	fmt.Printf(
		"Node %d received ping: %s\n",
		n.ID,
		args.Message,
	)

	reply.Message = "pong"

	return nil
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

}
