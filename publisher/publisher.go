package publisher

import "github.com/maksimru/event-scheduler/message"

type Publisher interface {
	Dispatch(message.Message) error
	Close() error
}
