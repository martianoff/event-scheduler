package listener

import (
	"context"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/prioritizer"
)

type Listener interface {
	Boot(context.Context, channel.Channel, *prioritizer.Prioritizer) error
	Listen() error
	Stop() error
}
