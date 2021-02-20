package clustermanager

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/maksimru/event-scheduler/errormessages"
	"github.com/maksimru/event-scheduler/nodenameresolver"
	"net/http"
)

type ClusterManager interface {
	Status(ctx echo.Context) error
	AddNode(ctx echo.Context) error
	DeleteNode(ctx echo.Context) error
	BootRaftClusterManager(cluster *raft.Raft, httpServer *echo.Echo) error
}

type RaftClusterManager struct {
	cluster    *raft.Raft
	httpServer *echo.Echo
}

func (m *RaftClusterManager) BootRaftClusterManager(cluster *raft.Raft, httpServer *echo.Echo) error {
	m.cluster = cluster
	m.httpServer = httpServer
	m.httpServer.GET("/cluster", m.Status)
	m.httpServer.DELETE("/cluster", m.DeleteNode)
	m.httpServer.POST("/cluster", m.AddNode)
	return nil
}

func (m *RaftClusterManager) Status(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"data": m.cluster.Stats(),
	})
}

type NodeInput struct {
	NodeAddr string `json:"node_addr" form:"node_addr" query:"node_addr" validate:"required"`
}

func (m *RaftClusterManager) AddNode(ctx echo.Context) error {
	if m.cluster.State() != raft.Leader {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error adding new channel %s: %s", ctx, errormessages.ErrOperationIsRestrictedOnNonLeader),
		})
	}

	node := new(NodeInput)

	if err := ctx.Bind(&node); err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error binding: %s", err.Error()),
		})
	}
	if err := ctx.Validate(node); err != nil {
		return echo.NewHTTPError(http.StatusNotAcceptable, err.Error())
	}
	f := m.cluster.AddVoter(nodenameresolver.Resolve(node.NodeAddr), raft.ServerAddress(node.NodeAddr), 0, 0)
	err := f.Error()
	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error adding new node %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": true,
		"data":   m.cluster.Stats(),
	})
}

func (m *RaftClusterManager) DeleteNode(ctx echo.Context) error {
	if m.cluster.State() != raft.Leader {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error adding new channel %s: %s", ctx, errormessages.ErrOperationIsRestrictedOnNonLeader),
		})
	}

	node := new(NodeInput)
	if err := ctx.Bind(&node); err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error binding: %s", err.Error()),
		})
	}
	if err := ctx.Validate(node); err != nil {
		return echo.NewHTTPError(http.StatusNotAcceptable, err.Error())
	}
	f := m.cluster.RemoveServer(nodenameresolver.Resolve(node.NodeAddr), 0, 0)
	err := f.Error()
	if err != nil {
		return ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"status": false,
			"error":  fmt.Sprintf("error removing node %s: %s", ctx, err.Error()),
		})
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": true,
		"data":   m.cluster.Stats(),
	})
}
