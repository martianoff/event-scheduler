package listener

import (
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
)

type Listener interface {
	Boot(config.Config, *goconcurrentqueue.FIFO) error
	Listen() error
}
