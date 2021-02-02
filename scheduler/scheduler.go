package scheduler

import (
	"context"
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
	"io/ioutil"
	"net"
	"os"
	"time"
)

const (
	raftSnapShotRetain      = 2
	raftLogCacheSize        = 256
	raftSnapshotThreshold   = 2024
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
	config       config.Config
	listener     listener.Listener
	publisher    publisher.Publisher
	processor    *processor.Processor
	prioritizer  *prioritizer.Prioritizer
	dataStorage  *storage.PqStorage
	inboundPool  *goconcurrentqueue.FIFO
	outboundPool *goconcurrentqueue.FIFO
	raftCluster  *raft.Raft
}

func NewScheduler(ctx context.Context, config config.Config) *Scheduler {
	scheduler := new(Scheduler)
	scheduler.config = config
	scheduler.inboundPool = goconcurrentqueue.NewFIFO()
	scheduler.outboundPool = goconcurrentqueue.NewFIFO()
	scheduler.dataStorage = storage.NewPqStorage()
	scheduler.BootPublisher(ctx)
	scheduler.BootPrioritizer(ctx)
	scheduler.BootListener(ctx)
	scheduler.BootProcessor(ctx)
	scheduler.BootCluster(ctx)
	return scheduler
}

func (s *Scheduler) Run(ctx context.Context) error {

	g, ctx := errgroup.WithContext(ctx)

	// listener receives scheduled jobs and saves them into the inboundPool
	g.Go(func() error {
		err := s.listener.Listen()
		if err != nil {
			log.Error("listener failure: ", err.Error())
		}
		return err
	})

	// prioritizer receives scheduled jobs from the inboundPool and moves them into data storage
	g.Go(func() error {
		err := s.prioritizer.Process()
		if err != nil {
			log.Error("prioritizer failure: ", err.Error())
		}
		return err
	})

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

	return g.Wait()
}

func (s *Scheduler) BootCluster(ctx context.Context) {
	path, _ := ioutil.TempDir("storage", "scheduler")
	store, err := raftbadger.NewBadgerStore(path)
	if err != nil {
		panic("exception during badger store boot: " + err.Error())
	}
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
	if err != nil {
		panic("exception during badger cache store boot: " + err.Error())
	}
	snapshotStore, err := raft.NewFileSnapshotStore("snapshots", raftSnapShotRetain, os.Stdout)
	if err != nil {
		panic("exception during snapshot store boot: " + err.Error())
	}
	raftTransportTcpAddr := "127.0.0.1:1101"
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftTransportTcpAddr)
	transport, err := raft.NewTCPTransport(raftTransportTcpAddr, tcpAddr, raftTransportMaxPool, raftTransportTcpTimeout, os.Stdout)
	if err != nil {
		panic("exception during tcp transport boot: " + err.Error())
	}
	raftconfig := raft.DefaultConfig()
	raftconfig.LogLevel = s.config.LogLevel
	nodeHost, _ := os.Hostname()
	raftconfig.LocalID = raft.ServerID(nodeHost)
	raftconfig.SnapshotThreshold = raftSnapshotThreshold
	raftServer, err := raft.NewRaft(raftconfig, fsm.NewPrioritizedFSM(s.dataStorage), cacheStore, store, snapshotStore, transport)
	if err != nil {
		panic("exception during raft clusterization boot: " + err.Error())
	}
	s.raftCluster = raftServer
	log.Info("cluster boot is finished: ", nodeHost)
}

func (s *Scheduler) BootProcessor(ctx context.Context) {
	processorInstance := new(processor.Processor)
	err := (*processorInstance).Boot(ctx, s.publisher, s.GetDataStorage())
	if err != nil {
		panic("exception during processor boot: " + err.Error())
	}
	s.processor = processorInstance
	log.Info("processor boot is finished")
}

func (s *Scheduler) BootPrioritizer(ctx context.Context) {
	prioritizerInstance := new(prioritizer.Prioritizer)
	err := (*prioritizerInstance).Boot(ctx, s.GetInboundPool(), s.GetDataStorage())
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
		err := listenerInstance.Boot(ctx, s.GetConfig(), s.GetInboundPool())
		if err != nil {
			panic("exception during listener boot: " + err.Error())
		}
		s.listener = listenerInstance
	case "test":
		listenerInstance := new(listenertest.Listener)
		err := listenerInstance.Boot(ctx, s.GetConfig(), s.GetInboundPool())
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
	default:
		log.Warn("selected publisher driver is not yet supported")
		panic("selected publisher driver is not yet supported")
	}
	log.Info("publisher boot is finished")
}

func (s *Scheduler) GetDataStorage() *storage.PqStorage {
	return s.dataStorage
}

func (s *Scheduler) GetInboundPool() *goconcurrentqueue.FIFO {
	return s.inboundPool
}

func (s *Scheduler) GetOutboundPool() *goconcurrentqueue.FIFO {
	return s.outboundPool
}

func (s *Scheduler) GetPublisher() *publisher.Publisher {
	return &s.publisher
}

func (s *Scheduler) GetConfig() config.Config {
	return s.config
}
