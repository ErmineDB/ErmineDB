package erminedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

var (
// log = hclog.New(&hclog.LoggerOptions{Name: "erminedb"})
)

type Server struct {
	PeerId    string // host:raftPort:httpPort TODO this may be an issue for IPV6 addresses
	BootPeers map[string]bool
	dataDir   string
	Raft      *raft.Raft
	Store     *ErmineDB
}

func parsePeer(peer string) (string, string) {
	peerCfg := strings.Split(peer, ":")
	return fmt.Sprintf("%s:%s", peerCfg[0], peerCfg[1]), peerCfg[2]
}

func NewServer(dataDir string, PeerId string, bootPeers []string) *Server {
	bootSet := make(map[string]bool)
	bootSet[PeerId] = true
	for _, peer := range bootPeers {
		bootSet[peer] = true
	}
	return &Server{PeerId: PeerId, dataDir: dataDir, BootPeers: bootSet}
}

// This will start the Raft node and will join the cluster after the end.
func (s *Server) Start() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(s.PeerId)
	raftAddr, _ := parsePeer(s.PeerId)

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return err
	}

	trans, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	if _, err := os.Stat(s.dataDir); os.IsNotExist(err) {
		if err = os.Mkdir(s.dataDir, 0755); err != nil {
			return err
		}
	}

	// badgerdir := fmt.Sprintf("%s/badger", s.dataDir)
	// if err := os.MkdirAll(badgerdir, 0755); err != nil {
	datadir := fmt.Sprintf("%s/ermine", s.dataDir)
	if err := os.MkdirAll(datadir, 0755); err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(s.dataDir, 2, os.Stderr)
	if err != nil {
		log.Info("Error creating snapshot store")
		return err
	}

	bst, err := NewErmineStore(datadir)
	if err != nil {
		log.Info("Error creating Ermine store")
		return err
	}

	log.Info("raftConfig: ", raftConfig)
	ra, err := raft.NewRaft(raftConfig, bst, bst, bst, snapshots, trans)
	if err != nil {
		log.Info("Error creating newraft")
		return err
	}

	s.Raft = ra
	s.Store = bst.db

	var servers []raft.Server
	for PeerId := range s.BootPeers {
		log.Info("Registering", "PeerId", PeerId)
		peerRaft, _ := parsePeer(PeerId)
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(PeerId),
			Address: raft.ServerAddress(peerRaft),
		})
	}

	// TODO may need to refactor how cluster is initialized.
	// i.e. adding 2 boot peers makes the node stop reacting to heartbeat timeout.
	s.Raft.BootstrapCluster(raft.Configuration{Servers: servers})

	return nil
}

func (s *Server) RaftSet(key string, value []byte) error {
	if log.IsDebug() {
		log.Debug("Log Set", "k", key, "v", value)
	}
	command := &VpLogCmd{Op: CMDSET, Key: key, Value: value}
	buff, err := json.Marshal(command)
	if err != nil {
		return err
	}
	future := s.Raft.Apply(buff, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (s *Server) RaftDelete(key string) error {
	if log.IsDebug() {
		log.Debug("Log del", "k", key)
	}
	// command := &eBadger.VpLogCmd{Op: eBadger.CMDDEL, Key: key}
	command := &VpLogCmd{Op: CMDDEL, Key: key}
	buff, err := json.Marshal(command)
	if err != nil {
		return err
	}
	future := s.Raft.Apply(buff, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (s *Server) RaftJoin(PeerId string) error {
	if s.Raft.State() != raft.Leader {
		return errors.New("not the leader")
	}
	configFuture := s.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	peerRaft, _ := parsePeer(PeerId)
	f := s.Raft.AddVoter(raft.ServerID(PeerId), raft.ServerAddress(peerRaft), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (s *Server) RaftLeave(PeerId string) error {
	if s.Raft.State() != raft.Leader {
		return errors.New("not the leader")
	}
	configFuture := s.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	future := s.Raft.RemoveServer(raft.ServerID(PeerId), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}
