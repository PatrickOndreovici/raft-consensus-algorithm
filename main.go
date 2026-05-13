package main

import (
	"raft-consensus-algorithm/raft"
	"time"
)

func main() {
	n1 := raft.NewNode(1, ":8001")
	n2 := raft.NewNode(2, ":8002")
	n3 := raft.NewNode(3, ":8003")

	go n1.Start()
	go n2.Start()
	go n3.Start()

	time.Sleep(time.Second)

	n1.ConnectToPeer(2, ":8002")
	n1.ConnectToPeer(3, ":8003")

	n2.ConnectToPeer(1, ":8001")
	n2.ConnectToPeer(3, ":8003")

	n3.ConnectToPeer(1, ":8001")
	n3.ConnectToPeer(2, ":8002")

	time.Sleep(time.Second)

	n1.SendPing(2, "hello from node1")
	n2.SendPing(3, "hello from node2")
	n3.SendPing(1, "hello from node3")

	select {}
}
