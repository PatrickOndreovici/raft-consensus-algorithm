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
	}
}

func (n *Node) run() {
	n.electionTimer = time.NewTimer(electionTimeout())

	for {
		select {

		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.State == Follower {
				n.State = Candidate
				n.mu.Unlock()
				go n.startElection()
			} else {
				n.mu.Unlock()
			}
			n.electionTimer.Reset(electionTimeout())

		case <-n.heartbeatCh:
			// Reset the election timer.
			// If the timer already expired before this heartbeat arrived,
			// a stale event may still be pending in the timer channel.
			// We drain it to prevent a false election.
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

			err := peer.Call("Node.RequestVote", RequestVoteArgs{
				Term:        localCurrentTerm,
				CandidateID: n.ID,
			}, &reply)

			if err != nil {
				return
			}

			if reply.Term > localCurrentTerm {
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

	clusterSize := len(n.Peers) + 1
	majority := clusterSize/2 + 1

	if numberOfVotes >= majority {
		n.mu.Lock()
		n.State = Leader
		go n.leaderSendHeartbeat()
		n.mu.Unlock()
	}
}

func (n *Node) leaderSendHeartbeat() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {

		n.mu.Lock()
		if n.State != Leader {
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
					return
				}

				n.mu.Lock()
				defer n.mu.Unlock()

				if n.State != Leader {
					return
				}

				if reply.Term > n.CurrentTerm {
					n.CurrentTerm = reply.Term
					n.State = Follower
					n.VotedFor = -1
					return
				}

			}(peerID, peer, term, leaderID)
		}
	}
}
