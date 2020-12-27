package scheduler

import (
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/listener"
	listenerpubsub "github.com/maksimru/event-scheduler/listener/pubsub"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"sync"
)

type StartableScheduler interface {
	Run() error
}

type BootableScheduler interface {
	BootProcessor()
	BootPrioritizer()
	BootListener()
	BootPublisher()
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

func NewScheduler(config config.Config) *Scheduler {
	scheduler := new(Scheduler)
	scheduler.config = config
	scheduler.inboundPool = goconcurrentqueue.NewFIFO()
	scheduler.outboundPool = goconcurrentqueue.NewFIFO()
	scheduler.dataStorage = storage.NewPqStorage()
	return scheduler
}

func (s *Scheduler) Run() error {
	s.BootPublisher()
	s.BootPrioritizer()
	s.BootListener()
	s.BootProcessor()

	var wg sync.WaitGroup

	// listener receives scheduled jobs and saves them into the inboundPool
	wg.Add(1)
	go func(listener *listener.Listener) {
		defer wg.Done()
		err := (*listener).Listen()
		if err != nil {
			log.Error("listener failure: ", err.Error())
			panic(err)
		}
	}(&s.listener)

	// prioritizer receives scheduled jobs from the inboundPool and moves them into data storage
	wg.Add(1)
	go func(prioritizer *prioritizer.Prioritizer) {
		defer wg.Done()
		err := prioritizer.Process()
		if err != nil {
			log.Error("prioritizer failure: ", err.Error())
			panic(err)
		}
	}(s.prioritizer)

	// processor checks data storage for scheduled jobs if they are ready to dispatch and move them to the outboundPool
	wg.Add(1)
	go func(processor *processor.Processor) {
		defer wg.Done()
		err := processor.Process()
		if err != nil {
			log.Error("processor failure: ", err.Error())
			panic(err)
		}
	}(s.processor)

	// publisher dispatches prepared jobs from the outboundPool to the destination queue
	wg.Add(1)
	go func(publisher *publisher.Publisher) {
		defer wg.Done()
		err := (*publisher).Dispatch()
		if err != nil {
			log.Error("publisher failure: ", err.Error())
			panic(err)
		}
	}(&s.publisher)

	wg.Wait()

	return nil
}

func (s *Scheduler) BootProcessor() {
	processorInstance := new(processor.Processor)
	err := (*processorInstance).Boot(s.publisher, s.GetDataStorage())
	if err != nil {
		panic("exception during processor boot: " + err.Error())
	}
	s.processor = processorInstance
	log.Info("processor boot is finished")
}

func (s *Scheduler) BootPrioritizer() {
	prioritizerInstance := new(prioritizer.Prioritizer)
	err := (*prioritizerInstance).Boot(s.GetInboundPool(), s.GetDataStorage())
	if err != nil {
		panic("exception during prioritizer boot: " + err.Error())
	}
	s.prioritizer = prioritizerInstance
	log.Info("prioritizer boot is finished")
}

func (s *Scheduler) BootListener() {
	switch s.config.ListenerDriver {
	case "pubsub":
		listenerInstance := new(listenerpubsub.ListenerPubsub)
		err := listenerInstance.Boot(s.GetConfig(), s.GetInboundPool())
		if err != nil {
			panic("exception during listener boot: " + err.Error())
		}
		s.listener = listenerInstance
	default:
		panic("selected listener driver is not yet supported")
	}
	log.Info("listener boot is finished")
}

func (s *Scheduler) BootPublisher() {
	switch s.config.PublisherDriver {
	case "pubsub":
		publisherInstance := new(publisherpubsub.PubsubPublisher)
		err := publisherInstance.Boot(s.GetConfig(), s.GetOutboundPool())
		if err != nil {
			panic("exception during publisher boot: " + err.Error())
		}
		s.publisher = publisherInstance
	default:
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
