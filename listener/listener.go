package listener

import (
	"context"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/prioritizer"
)

type Listener interface {
	Boot(context.Context, config.Config, *prioritizer.Prioritizer) error
	Listen() error
	Stop() error
}
