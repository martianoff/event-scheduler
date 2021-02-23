package publisher

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher interface {
	Boot(context.Context, channel.Channel, *goconcurrentqueue.FIFO) error
	Push(message.Message) error
	Dispatch() error
}
