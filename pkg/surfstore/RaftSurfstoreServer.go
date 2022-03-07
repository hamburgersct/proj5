package surfstore

import (
	context "context"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	lastApplied int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// volatile state on leaders
	nextIndex  []int64
	matchIndex []int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClients []RaftSurfstoreClient
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader {
		return &FileInfoMap{}, ERR_NOT_LEADER
	}
	return &FileInfoMap{
		FileInfoMap: s.metaStore.FileMetaMap,
	}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	if !s.isLeader {
		return &BlockStoreAddr{}, ERR_NOT_LEADER
	}
	// ???
	return &BlockStoreAddr{Addr: "localhost:8081"}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return &Version{}, ERR_NOT_LEADER
	}

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	go s.attemptCommit()

	success := <-committed
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) attemptCommit() {
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		// TODO handle crashed nodes
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log[:entryIdx+1],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output.Success {
			commitChan <- output
			return
		}
		// TODO update state. s.nextIndex, etc

		// TODO handle crashed/ non success cases
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// if crashed, return err
	if s.isCrashed {
		return &AppendEntryOutput{}, ERR_SERVER_CRASHED
	}

	// step down from leader, if AppendEntryOutput response is higher term

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	if input.Term > s.term {
		s.term = input.Term
	}

	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	// TODO only do this if leaderCommit > commitIndex
	s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	output.Success = true

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	// otherwise, set leader; term++, broadcast heartbeat
	s.isLeader = true
	s.term++
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	// not leader, do nothing
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	//  send a round of appendentries
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			// TODO figure out which entries to send
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if output != nil {
			// server is alive
		}
	}

	return &Success{Flag: true}, nil
}

// for testing purpose
func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
