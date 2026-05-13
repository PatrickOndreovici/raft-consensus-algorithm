package raft

import (
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type Node struct {
	mu sync.RWMutex

	ID    int
	Addr  string
	Peers map[int]*rpc.Client

	State       State
	CurrentTerm int
	VotedFor    int
}

func NewNode(id int, addr string) *Node {
	return &Node{
		ID:          id,
		Addr:        addr,
		Peers:       make(map[int]*rpc.Client),
		State:       Follower,
		CurrentTerm: 0,
		VotedFor:    -1,
	}
}

func (n *Node) runElectionTimer() {
	timeout := electionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		<-timer.C

		n.mu.Lock()
		if n.State != Leader {
			n.mu.Unlock()
			n.startElection()
			return
		}
		n.mu.Unlock()
		timer.Reset(electionTimeout())
	}
}

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.CurrentTerm++
	n.State = Candidate
	n.VotedFor = n.ID
	localCurrentTerm := n.CurrentTerm
	n.mu.Unlock()

	numberOfVotes := 1
	var voteMu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(n.Peers))

	for _, peer := range n.Peers {
		go func(peer *rpc.Client) {
			defer wg.Done()

			reply := RequestVoteReply{}

			err := peer.Call("Raft.RequestVote", RequestVoteArgs{
				Term:        localCurrentTerm,
				CandidateID: n.ID,
			}, &reply)

			if err != nil {
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				numberOfVotes++
				voteMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	if numberOfVotes > len(n.Peers)/2 {
		n.mu.Lock()
		n.State = Leader
		n.mu.Unlock()
	}
}
