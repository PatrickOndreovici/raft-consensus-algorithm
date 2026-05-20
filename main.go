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

	time.Sleep(time.Second * 2)

	n1.StartRaft()
	n2.StartRaft()
	n3.StartRaft()

	select {}
}
