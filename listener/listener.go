package listener

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
)

type Listener interface {
	Boot(context.Context, config.Config, *goconcurrentqueue.FIFO) error
	Listen() error
}
