package channelmanager

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/maksimru/event-scheduler/channel"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/config"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/config"
	"github.com/maksimru/event-scheduler/storage"
	"net/http"
)

type ChannelManagerServer interface {
	ListChannels(ctx echo.Context) error
	AddChannel(ctx echo.Context) error
	DeleteChannel(ctx echo.Context) error
	UpdateChannel(ctx echo.Context) error
}

type SchedulerChannelManagerServer struct {
	manager    ChannelManager
	httpServer *echo.Echo
}

func (m *SchedulerChannelManagerServer) BootChannelManagerServer(manager ChannelManager, httpServer *echo.Echo) error {
	m.manager = manager
	m.httpServer = httpServer
	m.initHttpRoutes()
	return nil
}

func (m *SchedulerChannelManagerServer) initHttpRoutes() {
	m.httpServer.GET("/channels", m.ListChannels)
	m.httpServer.DELETE("/channels/:id", m.DeleteChannel)
	m.httpServer.POST("/channels", m.AddChannel)
	m.httpServer.PATCH("/channels/:id", m.UpdateChannel)
}

type ChannelInput struct {
	Source      SourceInput `json:"source" form:"source" query:"source" validate:"required,dive"`
	Destination TargetInput `json:"destination" form:"destination" query:"destination" validate:"required,dive"`
}

type SourceInput struct {
	Driver             string                            `json:"driver" form:"driver" query:"driver" validate:"required,oneof=pubsub"`
	PubsubSourceConfig pubsublistenerconfig.SourceConfig `json:"config" form:"config" query:"config" validate:"required_if=Driver pubsub"`
}

type TargetInput struct {
	Driver             string                                  `json:"driver" form:"driver" query:"driver" validate:"required,oneof=pubsub"`
	PubsubTargetConfig pubsubpublisherconfig.DestinationConfig `json:"config" form:"config" query:"config" validate:"required_if=Driver pubsub"`
}

func (m *SchedulerChannelManagerServer) AddChannel(ctx echo.Context) error {
	c := new(ChannelInput)
	if err := ctx.Bind(&c); err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error binding: %s", err.Error()),
		})
	}
	if err := ctx.Validate(c); err != nil {
		return echo.NewHTTPError(http.StatusNotAcceptable, err.Error())
	}
	channelID, err := m.manager.AddChannel(channel.Channel{
		Source: channel.Source{
			Driver: c.Source.Driver,
			Config: pubsublistenerconfig.SourceConfig{
				ProjectID:      c.Source.PubsubSourceConfig.ProjectID,
				SubscriptionID: c.Source.PubsubSourceConfig.SubscriptionID,
				KeyFile:        c.Source.PubsubSourceConfig.KeyFile,
			},
		},
		Destination: channel.Destination{
			Driver: c.Destination.Driver,
			Config: pubsubpublisherconfig.DestinationConfig{
				ProjectID: c.Destination.PubsubTargetConfig.ProjectID,
				TopicID:   c.Destination.PubsubTargetConfig.TopicID,
				KeyFile:   c.Destination.PubsubTargetConfig.KeyFile,
			},
		},
	})
	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error adding new channel %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status":    true,
		"channelID": channelID,
	})
}

func (m *SchedulerChannelManagerServer) UpdateChannel(ctx echo.Context) error {
	channelID := ctx.Param("id")
	c := new(ChannelInput)
	if err := ctx.Bind(&c); err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error binding: %s", err.Error()),
		})
	}
	if err := ctx.Validate(c); err != nil {
		return echo.NewHTTPError(http.StatusNotAcceptable, err.Error())
	}
	_, err := m.manager.UpdateChannel(channelID, channel.Channel{
		Source: channel.Source{
			Driver: c.Source.Driver,
			Config: pubsublistenerconfig.SourceConfig{
				ProjectID:      c.Source.PubsubSourceConfig.ProjectID,
				SubscriptionID: c.Source.PubsubSourceConfig.SubscriptionID,
				KeyFile:        c.Source.PubsubSourceConfig.KeyFile,
			},
		},
		Destination: channel.Destination{
			Driver: c.Destination.Driver,
			Config: pubsubpublisherconfig.DestinationConfig{
				ProjectID: c.Destination.PubsubTargetConfig.ProjectID,
				TopicID:   c.Destination.PubsubTargetConfig.TopicID,
				KeyFile:   c.Destination.PubsubTargetConfig.KeyFile,
			},
		},
	})
	if err == storage.ErrChannelNotFound {
		return ctx.JSON(http.StatusNotFound, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error updating channel %s: %s", ctx, err.Error()),
		})
	}
	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error updating channel %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status":    true,
		"channelID": channelID,
	})
}

func (m *SchedulerChannelManagerServer) DeleteChannel(ctx echo.Context) error {
	channelID := ctx.Param("id")
	err := m.manager.DeleteChannel(channelID)

	if err == storage.ErrChannelNotFound {
		return ctx.JSON(http.StatusNotFound, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error removing channel %s: %s", ctx, err.Error()),
		})
	}

	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error removing channel %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status":    true,
		"channelID": channelID,
	})
}

func (m *SchedulerChannelManagerServer) ListChannels(ctx echo.Context) error {
	channels, err := m.manager.GetChannels()
	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error removing channel %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": true,
		"data":   channels,
	})
}
