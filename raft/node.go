package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type Node struct {
	mu sync.Mutex

	ID    int
	Addr  string
	Peers map[int]*rpc.Client

	State       State
	CurrentTerm int
	VotedFor    int

	heartbeatCh   chan struct{}
	electionTimer *time.Timer
}

func NewNode(id int, addr string) *Node {
	return &Node{
		ID:          id,
		Addr:        addr,
		Peers:       make(map[int]*rpc.Client),
		State:       Follower,
		CurrentTerm: 0,
		VotedFor:    -1,
		heartbeatCh: make(chan struct{}, 1),
	}
}

func (n *Node) run() {
	n.electionTimer = time.NewTimer(electionTimeout())

	for {
		select {

		case <-n.electionTimer.C:
			n.mu.Lock()
			shouldStartElection := n.State != Leader
			n.mu.Unlock()

			if shouldStartElection {
				go n.startElection()
			}

			n.electionTimer.Reset(electionTimeout())

		case <-n.heartbeatCh:
			if !n.electionTimer.Stop() {
				select {
				case <-n.electionTimer.C:
				default:
				}
			}
			n.electionTimer.Reset(electionTimeout())
		}
	}
}

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.State == Leader {
		n.mu.Unlock()
		return
	}
	n.CurrentTerm++
	n.State = Candidate
	n.VotedFor = n.ID
	localCurrentTerm := n.CurrentTerm
	n.mu.Unlock()

	log.Printf("[Node %d] election started (term=%d)", n.ID, localCurrentTerm)

	numberOfVotes := 1
	var voteMu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(n.Peers))

	for peerID, peer := range n.Peers {
		go func(peerID int, peer *rpc.Client) {
			defer wg.Done()

			reply := RequestVoteReply{}

			err := peer.Call("RpcHandler.RequestVote", RequestVoteArgs{
				Term:        localCurrentTerm,
				CandidateID: n.ID,
			}, &reply)

			if err != nil {
				log.Printf("[Node %d] RequestVote to Node %d failed: %v", n.ID, peerID, err)
				return
			}

			if reply.Term > localCurrentTerm {
				log.Printf("[Node %d] saw higher term %d from Node %d → reverting to follower", n.ID, reply.Term, peerID)
				n.mu.Lock()
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				log.Printf("[Node %d] got vote from Node %d (term=%d)", n.ID, peerID, localCurrentTerm)
				voteMu.Lock()
				numberOfVotes++
				voteMu.Unlock()
			} else {
				log.Printf("[Node %d] vote denied by Node %d (term=%d)", n.ID, peerID, localCurrentTerm)
			}
		}(peerID, peer)
	}

	wg.Wait()

	clusterSize := len(n.Peers) + 1
	majority := clusterSize/2 + 1

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Candidate || n.CurrentTerm != localCurrentTerm {
		log.Printf("[Node %d] election result discarded — state changed during election (state=%v term=%d)", n.ID, n.State, n.CurrentTerm)
		return
	}

	if numberOfVotes >= majority {
		log.Printf("[Node %d] won election (term=%d, votes=%d/%d)", n.ID, localCurrentTerm, numberOfVotes, clusterSize)
		n.State = Leader
		go n.leaderSendHeartbeat()
	} else {
		log.Printf("[Node %d] lost election (term=%d, votes=%d/%d)", n.ID, localCurrentTerm, numberOfVotes, clusterSize)
	}
}

func (n *Node) leaderSendHeartbeat() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.State != Leader {
			log.Printf("[Node %d] no longer leader, stopping heartbeats", n.ID)
			n.mu.Unlock()
			return
		}

		term := n.CurrentTerm
		leaderID := n.ID
		n.mu.Unlock()

		for peerID, peer := range n.Peers {
			go func(peerID int, peer *rpc.Client, term int, leaderID int) {
				args := AppendEntriesArgs{
					Term:     term,
					LeaderID: leaderID,
				}

				var reply AppendEntriesReply

				if err := peer.Call("RpcHandler.AppendEntries", args, &reply); err != nil {
					log.Printf("[Node %d] heartbeat to Node %d failed: %v", leaderID, peerID, err)
					return
				}

				n.mu.Lock()
				defer n.mu.Unlock()

				if n.State != Leader {
					return
				}

				if reply.Term > n.CurrentTerm {
					log.Printf("[Node %d] saw higher term %d from Node %d → stepping down", leaderID, reply.Term, peerID)
					n.becomeFollower(reply.Term)
				}
			}(peerID, peer, term, leaderID)
		}
	}
}

func (n *Node) becomeFollower(term int) {
	log.Printf("[Node %d] became follower (term=%d)", n.ID, term)
	n.State = Follower
	n.VotedFor = -1
	n.CurrentTerm = term
}

func (n *Node) StartRaft() {
	go n.run()
}
