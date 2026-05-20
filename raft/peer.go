package raft

import (
	"fmt"
	"log"
	"net/rpc"
)

func (n *Node) ConnectToPeer(id int, addr string) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	n.Peers[id] = client

	log.Printf("Node %d connected to node %d\n",
		n.ID,
		id)
	
}

func (n *Node) SendPing(peerID int, message string) {
	client := n.Peers[peerID]

	var reply PingReply

	err := client.Call(
		"RpcHandler.Ping",
		PingArgs{Message: message},
		&reply,
	)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf(
		"Node %d got reply from node %d: %s\n",
		n.ID,
		peerID,
		reply.Message,
	)
}
