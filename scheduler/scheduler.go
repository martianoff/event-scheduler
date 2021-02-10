package scheduler

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/BBVA/raft-badger"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/listener"
	listenerpubsub "github.com/maksimru/event-scheduler/listener/pubsub"
	listenertest "github.com/maksimru/event-scheduler/listener/test"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	publishertest "github.com/maksimru/event-scheduler/publisher/test"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"strings"
	"time"
)

const (
	raftSnapShotRetain      = 2
	raftLogCacheSize        = 256
	raftSnapshotThreshold   = 2048
	raftTransportTcpTimeout = 10 * time.Second
	raftTransportMaxPool    = 10
)

type StartableScheduler interface {
	Run(ctx context.Context) error
}

type InitiableScheduler interface {
	BootProcessor(ctx context.Context)
	BootPrioritizer(ctx context.Context)
	BootListener(ctx context.Context)
	BootPublisher(ctx context.Context)
}

type Scheduler struct {
	config          config.Config
	listener        listener.Listener
	publisher       publisher.Publisher
	processor       *processor.Processor
	prioritizer     *prioritizer.Prioritizer
	dataStorage     *storage.PqStorage
	outboundPool    *goconcurrentqueue.FIFO
	raftCluster     *raft.Raft
	listenerRunning bool
}

func NewScheduler(ctx context.Context, config config.Config) *Scheduler {
	scheduler := new(Scheduler)
	scheduler.config = config
	scheduler.outboundPool = goconcurrentqueue.NewFIFO()
	scheduler.dataStorage = storage.NewPqStorage()
	scheduler.BootCluster(ctx)
	scheduler.BootPrioritizer(ctx)
	scheduler.BootListener(ctx)
	scheduler.BootPublisher(ctx)
	scheduler.BootProcessor(ctx)
	scheduler.listenerRunning = false
	return scheduler
}

func (s *Scheduler) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// processor checks data storage for scheduled jobs if they are ready to dispatch and move them to the outboundPool
	g.Go(func() error {
		err := s.processor.Process()
		if err != nil {
			log.Error("processor failure: ", err.Error())
		}
		return err
	})

	// publisher dispatches prepared jobs from the outboundPool to the destination queue
	g.Go(func() error {
		err := s.publisher.Dispatch()
		if err != nil {
			log.Error("publisher failure: ", err.Error())
		}
		return err
	})

	// stop cluster on context termination
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				log.Warn("context is done, stopping the cluster")
				s.raftCluster.Shutdown()
				return nil
			default:
				time.Sleep(time.Second)
			}
		}
	})

	return g.Wait()
}

func (s *Scheduler) BootCluster(ctx context.Context) {
	store, err := raftbadger.NewBadgerStore(s.config.StoragePath)
	if err != nil {
		panic("exception during badger store boot: " + err.Error())
	}
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
	if err != nil {
		panic("exception during badger cache store boot: " + err.Error())
	}
	snapshotStore, err := raft.NewFileSnapshotStore(s.config.StoragePath, raftSnapShotRetain, os.Stdout)
	if err != nil {
		panic("exception during snapshot store boot: " + err.Error())
	}
	raftTransportTcpAddr := s.config.ClusterNodeHost + ":" + s.config.ClusterNodePort
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftTransportTcpAddr)
	if err != nil {
		panic("unable to resolve cluster node address: " + err.Error())
	}
	transport, err := raft.NewTCPTransport(raftTransportTcpAddr, tcpAddr, raftTransportMaxPool, raftTransportTcpTimeout, os.Stdout)
	if err != nil {
		panic("exception during tcp transport boot: " + err.Error())
	}
	raftconfig := raft.DefaultConfig()
	raftconfig.LogLevel = s.config.LogLevel
	raftconfig.LocalID = raft.ServerID(getMD5Hash(raftTransportTcpAddr))
	raftconfig.SnapshotThreshold = raftSnapshotThreshold
	raftServer, err := raft.NewRaft(raftconfig, fsm.NewPrioritizedFSM(s.dataStorage), cacheStore, store, snapshotStore, transport)
	if err != nil {
		panic("exception during raft clusterization boot: " + err.Error())
	}
	s.raftCluster = raftServer
	log.Info("cluster boot is finished: ", raftconfig.LocalID)
	s.WatchCluster(ctx)

	// Init initial state
	initialClusterNodes := strings.Split(s.config.ClusterInitialNodes, ",")
	initialClusterServers := make([]raft.Server, len(initialClusterNodes))
	for k, nodeHost := range initialClusterNodes {
		// set other nodes to pending status
		suffrage := raft.Staging
		// set initial leader
		if nodeHost == s.config.ClusterInitialLeader {
			suffrage = raft.Voter
		}
		initialClusterServers[k] = raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(getMD5Hash(nodeHost)),
			Address:  raft.ServerAddress(nodeHost),
		}
	}
	s.raftCluster.BootstrapCluster(raft.Configuration{Servers: initialClusterServers})
}

func getMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func (s *Scheduler) WatchCluster(ctx context.Context) {
	// Watch for cluster leader changes
	go func(ctx context.Context, cluster *Scheduler) {
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-s.raftCluster.LeaderCh():
				s.ClusterLeaderChangeCallback(ctx, v)
			}
		}
	}(ctx, s)
}

func (s *Scheduler) ClusterLeaderChangeCallback(ctx context.Context, isLeader bool) {
	// run watcher when listener is booted only
	if s.listener != nil {
		if !isLeader && s.listenerRunning {
			log.Info("listener stopping")
			err := s.listener.Stop()
			if err != nil {
				log.Error("listener stop failure: ", err.Error())
			}
		} else if isLeader && !s.listenerRunning {
			func(s *Scheduler) {
				s.listenerRunning = true
				log.Info("listener starting")
				// listener reads messages from inbound queue and persists them in storage
				err := s.listener.Listen()
				s.listenerRunning = false
				if err != nil {
					log.Error("listener failure: ", err.Error())
				}
			}(s)
		}
	}
}

func (s *Scheduler) BootProcessor(ctx context.Context) {
	processorInstance := new(processor.Processor)
	err := (*processorInstance).Boot(ctx, s.publisher, s.GetDataStorage(), s.GetCluster())
	if err != nil {
		panic("exception during processor boot: " + err.Error())
	}
	s.processor = processorInstance
	log.Info("processor boot is finished")
}

func (s *Scheduler) BootPrioritizer(ctx context.Context) {
	prioritizerInstance := new(prioritizer.Prioritizer)
	err := (*prioritizerInstance).Boot(s.GetCluster())
	if err != nil {
		panic("exception during prioritizer boot: " + err.Error())
	}
	s.prioritizer = prioritizerInstance
	log.Info("prioritizer boot is finished")
}

func (s *Scheduler) BootListener(ctx context.Context) {
	switch s.config.ListenerDriver {
	case "pubsub":
		listenerInstance := new(listenerpubsub.Listener)
		err := listenerInstance.Boot(ctx, s.GetConfig(), s.prioritizer)
		if err != nil {
			panic("exception during listener boot: " + err.Error())
		}
		s.listener = listenerInstance
	case "test":
		listenerInstance := new(listenertest.Listener)
		err := listenerInstance.Boot(ctx, s.GetConfig(), s.prioritizer)
		if err != nil {
			panic("exception during listener boot: " + err.Error())
		}
		s.listener = listenerInstance
	default:
		log.Warn("selected listener driver is not yet supported")
		panic("selected listener driver is not yet supported")
	}
	log.Info("listener boot is finished")
}

func (s *Scheduler) BootPublisher(ctx context.Context) {
	switch s.config.PublisherDriver {
	case "pubsub":
		publisherInstance := new(publisherpubsub.Publisher)
		err := publisherInstance.Boot(ctx, s.GetConfig(), s.GetOutboundPool())
		if err != nil {
			panic("exception during publisher boot: " + err.Error())
		}
		s.publisher = publisherInstance
	case "test":
		publisherInstance := new(publishertest.Publisher)
		err := publisherInstance.Boot(ctx, s.GetConfig(), s.GetOutboundPool())
		if err != nil {
			panic("exception during publisher boot: " + err.Error())
		}
		s.publisher = publisherInstance
	case "test_w":
		publisherInstance := new(publishertest.Publisher)
		err := publisherInstance.Boot(ctx, s.GetConfig(), s.GetOutboundPool())
		if err != nil {
			panic("exception during publisher boot: " + err.Error())
		}
		publisherInstance.WrongConfig()
		s.publisher = publisherInstance
	default:
		log.Warn("selected publisher driver is not yet supported")
		panic("selected publisher driver is not yet supported")
	}
	log.Info("publisher boot is finished")
}

func (s *Scheduler) GetDataStorage() *storage.PqStorage {
	return s.dataStorage
}

func (s *Scheduler) GetOutboundPool() *goconcurrentqueue.FIFO {
	return s.outboundPool
}

func (s *Scheduler) GetPublisher() *publisher.Publisher {
	return &s.publisher
}

func (s *Scheduler) GetListener() *listener.Listener {
	return &s.listener
}

func (s *Scheduler) GetCluster() *raft.Raft {
	return s.raftCluster
}

func (s *Scheduler) GetConfig() config.Config {
	return s.config
}
