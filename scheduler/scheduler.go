package scheduler

import (
	"context"
	"github.com/BBVA/raft-badger"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/go-playground/validator/v10"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	nativemiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/channelmanager"
	"github.com/maksimru/event-scheduler/clustermanager"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/dispatcher"
	"github.com/maksimru/event-scheduler/errormessages"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/httpvalidator"
	"github.com/maksimru/event-scheduler/listener"
	listenerpubsub "github.com/maksimru/event-scheduler/listener/pubsub"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/pubsub/config"
	listenertest "github.com/maksimru/event-scheduler/listener/test"
	testlistenerconfig "github.com/maksimru/event-scheduler/listener/test/config"
	"github.com/maksimru/event-scheduler/middleware"
	"github.com/maksimru/event-scheduler/nodenameresolver"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
	testpublisherconfig "github.com/maksimru/event-scheduler/publisher/test/config"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"net"
	"net/http"
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

const (
	defaultChannelName = "default"
)

type StartableScheduler interface {
	Run(ctx context.Context) error
}

type InitiableScheduler interface {
	BootHttpServer(ctx context.Context)
	BootCluster(ctx context.Context)
	BootProcessor(ctx context.Context, c channel.Channel)
	BootPrioritizer(ctx context.Context)
	BootListener(ctx context.Context, c channel.Channel) error
	BootRaftManager(ctx context.Context)
	BootChannelManager(ctx context.Context)
}

type Scheduler struct {
	config           config.Config
	listeners        map[string]listener.Listener
	listenerRunning  map[string]bool
	processors       map[string]*processor.Processor
	processorRunning map[string]bool
	dispatcher       dispatcher.Dispatcher
	prioritizer      *prioritizer.Prioritizer
	dataStorage      *storage.PqStorage
	outboundPool     *goconcurrentqueue.FIFO
	raftCluster      *raft.Raft
	raftManager      clustermanager.ClusterManager
	channelManager   channelmanager.ChannelManager
	httpServer       *echo.Echo
	channelHandler   *channel.EventHandler
}

func NewScheduler(ctx context.Context, config config.Config) *Scheduler {
	scheduler := new(Scheduler)
	scheduler.config = config
	scheduler.outboundPool = goconcurrentqueue.NewFIFO()
	scheduler.dataStorage = storage.NewPqStorage()
	scheduler.dispatcher = dispatcher.NewDispatcher(ctx, scheduler.outboundPool, scheduler.dataStorage)
	scheduler.channelHandler = channel.NewEventHandler(scheduler.channelUpdated(ctx), scheduler.channelDeleted(ctx), scheduler.channelAdded(ctx))
	scheduler.BootHttpServer(ctx)
	scheduler.BootCluster(ctx)
	scheduler.BootPrioritizer(ctx)
	scheduler.BootRaftManager(ctx)
	scheduler.BootChannelManager(ctx)
	scheduler.listenerRunning = make(map[string]bool)
	scheduler.listeners = make(map[string]listener.Listener)
	scheduler.processorRunning = make(map[string]bool)
	scheduler.processors = make(map[string]*processor.Processor)
	return scheduler
}

func (s *Scheduler) isListenerRunning(channelID string) bool {
	status, has := s.listenerRunning[channelID]
	return status && has
}

func (s *Scheduler) isProcessorRunning(channelID string) bool {
	status, has := s.processorRunning[channelID]
	return status && has
}

func (s *Scheduler) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// webserver for api requests
	g.Go(func() error {
		err := s.httpServer.Start(":" + s.config.APIPort)
		if err != nil && err != http.ErrServerClosed {
			log.Error("http server failure: ", err.Error())
		}
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	})

	// publisher dispatches prepared jobs from the outboundPool to the destination queue
	g.Go(func() error {
		err := s.dispatcher.Dispatch()
		if err != nil {
			log.Error("dispatcher failure: ", err.Error())
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
			}
		}
	})

	// stop cluster on context termination
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				log.Warn("context is done, stopping the webserver")
				err := s.httpServer.Shutdown(ctx)
				if err != nil && err != context.DeadlineExceeded {
					log.Error("http server stop failure: ", err.Error())
				}
				return nil
			}
		}
	})

	return g.Wait()
}

func (s *Scheduler) BootHttpServer(ctx context.Context) {
	s.httpServer = echo.New()
	s.httpServer.HideBanner = true
	s.httpServer.HidePort = true
	s.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}
	s.httpServer.Pre(nativemiddleware.RemoveTrailingSlash())
	s.httpServer.Use(middleware.NewLeaderProxy(s.raftCluster, s.config).Process)
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
	raftconfig.LocalID = nodenameresolver.Resolve(raftTransportTcpAddr)
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
		initialClusterServers[k] = raft.Server{
			Suffrage: raft.Voter,
			ID:       nodenameresolver.Resolve(raftTransportTcpAddr),
			Address:  raft.ServerAddress(nodeHost),
		}
	}
	s.raftCluster.BootstrapCluster(raft.Configuration{Servers: initialClusterServers})
}

func (s *Scheduler) WatchCluster(ctx context.Context) {
	// Watch for cluster leader changes
	go func(ctx context.Context, cluster *Scheduler) {
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-s.raftCluster.LeaderCh():
				s.clusterLeaderChangeCallback(ctx, v)
			}
		}
	}(ctx, s)
}

// automatically creates single channel from env vars, must be called on leader
func (s *Scheduler) createDefaultChannel() {
	_, err := s.dataStorage.GetChannel(defaultChannelName)
	if err == storage.ErrChannelNotFound {
		if s.GetConfig().ListenerDriver != "" && s.GetConfig().PublisherDriver != "" {
			var srcCfg channel.SourceConfig
			switch s.GetConfig().ListenerDriver {
			case "pubsub":
				srcCfg = pubsublistenerconfig.SourceConfig{
					ProjectID:      s.GetConfig().PubsubListenerProjectID,
					SubscriptionID: s.GetConfig().PubsubListenerSubscriptionID,
					KeyFile:        s.GetConfig().PubsubListenerKeyFile,
				}
			case "test":
				srcCfg = testlistenerconfig.SourceConfig{}
			}
			var dstCfg channel.DestinationConfig
			switch s.GetConfig().PublisherDriver {
			case "pubsub":
				dstCfg = pubsubpublisherconfig.DestinationConfig{
					ProjectID: s.GetConfig().PubsubPublisherProjectID,
					TopicID:   s.GetConfig().PubsubPublisherTopicID,
					KeyFile:   s.GetConfig().PubsubPublisherKeyFile,
				}
			case "test":
				dstCfg = testpublisherconfig.DestinationConfig{}
			}
			_, err := s.channelManager.AddChannel(channel.Channel{
				ID: defaultChannelName,
				Source: channel.Source{
					Driver: s.GetConfig().ListenerDriver,
					Config: srcCfg,
				},
				Destination: channel.Destination{
					Driver: s.GetConfig().PublisherDriver,
					Config: dstCfg,
				},
			})
			if err != nil {
				log.Warn("default channel creation failed: ", err)
			}
		}
	}
}

func (s *Scheduler) clusterLeaderChangeCallback(ctx context.Context, isLeader bool) {
	if !isLeader {
		// listeners
		go func(s *Scheduler) {
			log.Info("listener stopping")
			for channelID, l := range s.listeners {
				if s.isListenerRunning(channelID) {
					err := l.Stop()
					if err != nil {
						log.Error("listener stop failure: ", err.Error(), ", channel ", channelID)
					}
				}
			}
		}(s)
		// processors
		go func(s *Scheduler) {
			log.Info("processor stopping")
			for channelID, p := range s.processors {
				if s.isProcessorRunning(channelID) {
					err := p.Stop()
					if err != nil {
						log.Error("processor stop failure: ", err.Error(), ", channel ", channelID)
					}
				}
			}
		}(s)
	} else if isLeader {
		s.createDefaultChannel()
		go func(s *Scheduler) {
			for _, c := range s.dataStorage.GetChannels() {
				s.ensureChannelStart(ctx, c)
			}
		}(s)
	}
}

func (s *Scheduler) ensureChannelStart(ctx context.Context, c channel.Channel) {
	// listener
	go func(s *Scheduler) {
		if s.isListenerRunning(c.ID) {
			return
		}
		s.listenerRunning[c.ID] = true
		log.Info("listener starting")
		err := s.BootListener(ctx, c)
		if err != nil {
			s.listenerRunning[c.ID] = false
			log.Info("listener unexpectedly stopped ", err.Error())
			return
		}
		err = s.listeners[c.ID].Listen()
		s.listenerRunning[c.ID] = false
		if err != nil {
			log.Error("listener failure: ", err.Error(), ", channel ", c.ID)
		}
	}(s)
	// processor
	go func(s *Scheduler) {
		for _, c := range s.dataStorage.GetChannels() {
			if s.isProcessorRunning(c.ID) {
				return
			}
			s.processorRunning[c.ID] = true
			log.Info("processor starting")
			s.BootProcessor(ctx, c)
			err := s.processors[c.ID].Process()
			s.processorRunning[c.ID] = false
			if err != nil {
				log.Error("processor failure: ", err.Error(), ", channel ", c.ID)
			}
		}
	}(s)
}

func (s *Scheduler) ensureChannelRestart(ctx context.Context, c channel.Channel) {
	go func(s *Scheduler) {
		if s.isListenerRunning(c.ID) {
			l := s.listeners[c.ID]
			err := l.Stop()
			if err != nil {
				log.Error("listener stop failure: ", err.Error(), ", channel ", c.ID)
			}
		}
		err := s.BootListener(ctx, c)
		if err != nil {
			s.listenerRunning[c.ID] = false
			log.Info("listener unexpectedly stopped ", err.Error())
			return
		}
		err = s.listeners[c.ID].Listen()
		s.listenerRunning[c.ID] = false
		if err != nil {
			log.Error("listener failure: ", err.Error(), ", channel ", c.ID)
		}
	}(s)
	// processor
	go func(s *Scheduler) {
		if s.isProcessorRunning(c.ID) {
			p := s.processors[c.ID]
			err := p.Stop()
			if err != nil {
				log.Error("processor stop failure: ", err.Error(), ", channel ", c.ID)
			}
			s.BootProcessor(ctx, c)
			err = s.processors[c.ID].Process()
			s.processorRunning[c.ID] = false
			if err != nil {
				log.Error("processor failure: ", err.Error(), ", channel ", c.ID)
			}
		}
	}(s)
}

func (s *Scheduler) ensureChannelStop(ctx context.Context, c channel.Channel) {
	// listener
	go func(s *Scheduler) {
		if s.isListenerRunning(c.ID) {
			l := s.listeners[c.ID]
			err := l.Stop()
			if err != nil {
				log.Error("listener stop failure: ", err.Error(), ", channel ", c.ID)
			}
		}
	}(s)
	// processor
	go func(s *Scheduler) {
		if s.isProcessorRunning(c.ID) {
			p := s.processors[c.ID]
			err := p.Stop()
			if err != nil {
				log.Error("processor stop failure: ", err.Error(), ", channel ", c.ID)
			}
		}
	}(s)
}

func (s *Scheduler) channelUpdated(ctx context.Context) func(channel.Channel) {
	return func(c channel.Channel) {
		log.Info("channel event handler channel updated, id ", c.ID)
		if s.raftCluster.State() == raft.Leader {
			s.ensureChannelRestart(ctx, c)
		}
	}
}

func (s *Scheduler) channelDeleted(ctx context.Context) func(channel.Channel) {
	return func(c channel.Channel) {
		log.Info("channel event handler channel removed, id ", c.ID)
		if s.raftCluster.State() == raft.Leader {
			s.ensureChannelStop(ctx, c)
		}
	}
}

func (s *Scheduler) channelAdded(ctx context.Context) func(channel.Channel) {
	return func(c channel.Channel) {
		log.Info("channel event handler channel added, id ", c.ID)
		if s.raftCluster.State() == raft.Leader {
			s.ensureChannelStart(ctx, c)
		}
	}
}

func (s *Scheduler) BootProcessor(ctx context.Context, channel channel.Channel) {
	processorInstance := new(processor.Processor)
	err := (*processorInstance).Boot(ctx, s.dispatcher, s.GetDataStorage(), s.GetCluster(), channel)
	if err != nil {
		panic("exception during processor boot: " + err.Error() + ", channel " + channel.ID)
	}
	s.processors[channel.ID] = processorInstance
	log.Info("processor boot is finished, channel ", channel.ID)
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

func (s *Scheduler) BootRaftManager(ctx context.Context) {
	manager := new(clustermanager.RaftClusterManager)
	err := manager.BootRaftClusterManager(s.raftCluster, s.httpServer)
	if err != nil {
		panic("exception during raft manager boot: " + err.Error())
	}
	s.raftManager = manager
	log.Info("raft manager boot is finished")
}

func (s *Scheduler) BootChannelManager(ctx context.Context) {
	manager := new(channelmanager.SchedulerChannelManager)
	if err := manager.BootChannelManager(s.raftCluster, s.dataStorage, s.channelHandler); err != nil {
		panic("exception during channel manager boot: " + err.Error())
	}
	log.Info("channel manager boot is finished")
	s.channelManager = manager
	server := new(channelmanager.SchedulerChannelManagerServer)
	if err := server.BootChannelManagerServer(manager, s.httpServer); err != nil {
		panic("exception during channel manager server boot: " + err.Error())
	}
	log.Info("channel manager server boot is finished")
}

func (s *Scheduler) BootListener(ctx context.Context, channel channel.Channel) error {
	switch channel.Source.Driver {
	case "pubsub":
		listenerInstance := new(listenerpubsub.Listener)
		err := listenerInstance.Boot(ctx, channel, s.prioritizer)
		if err != nil {
			panic("exception during listener boot: " + err.Error() + ", channel " + channel.ID)
		}
		s.listeners[channel.ID] = listenerInstance
	case "test":
		listenerInstance := new(listenertest.Listener)
		err := listenerInstance.Boot(ctx, channel, s.prioritizer)
		if err != nil {
			panic("exception during listener boot: " + err.Error() + ", channel " + channel.ID)
		}
		s.listeners[channel.ID] = listenerInstance
	default:
		log.Warn("selected listener driver is not yet supported, channel ", channel.ID)
		return errormessages.ErrSourceDriverNotSupported
	}
	log.Info("listener boot is finished, channel ", channel.ID)
	return nil
}

func (s *Scheduler) GetDataStorage() *storage.PqStorage {
	return s.dataStorage
}

func (s *Scheduler) GetOutboundPool() *goconcurrentqueue.FIFO {
	return s.outboundPool
}

func (s *Scheduler) GetDispatcher() *dispatcher.Dispatcher {
	return &s.dispatcher
}

func (s *Scheduler) GetCluster() *raft.Raft {
	return s.raftCluster
}

func (s *Scheduler) GetConfig() config.Config {
	return s.config
}
