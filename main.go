package main

import (
	"encoding/json"
	"flag"
	"raft-consensus-algorithm/raft"
	"time"
)

type Peer struct {
	ID      int    `json:"id"`
	Address string `json:"address"`
}

func main() {
	id := flag.Int("id", 0, "node id")
	address := flag.String("address", ":8000", "node address")
	peersJson := flag.String("peers", "[]", "peers json")
	flag.Parse()

	if *id == 0 || *address == "" {
		panic("id and address are required")
	}

	var peers []Peer

	err := json.Unmarshal([]byte(*peersJson), &peers)
	if err != nil {
		panic(err)
	}

	node := raft.NewNode(*id, *address)

	go node.Start()

	time.Sleep(time.Second * 15)

	for _, p := range peers {
		node.ConnectToPeer(p.ID, p.Address)
	}

	time.Sleep(2 * time.Second)

	node.StartRaft()

	select {}
}
