package scheduler

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
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
