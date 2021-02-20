package middleware

import (
	"bytes"
	"context"
	"github.com/go-playground/validator/v10"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	nativemiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/maksimru/event-scheduler/clustermanager"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/httpvalidator"
	"github.com/maksimru/event-scheduler/nodenameresolver"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

func inmemConfig() *raft.Config {
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	return conf
}

func bootStagingCluster(config config.Config) (*raft.Raft, *raft.NetworkTransport) {
	store := raft.NewInmemStore()
	cacheStore, _ := raft.NewLogCache(128, store)
	snapshotStore := raft.NewInmemSnapshotStore()
	raftTransportTcpAddr := config.ClusterNodeHost + ":" + config.ClusterNodePort
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftTransportTcpAddr)
	if err != nil {
		panic("unable to resolve cluster node address: " + err.Error())
	}
	transport, err := raft.NewTCPTransport(raftTransportTcpAddr, tcpAddr, 10, time.Second*10, os.Stdout)
	if err != nil {
		panic("exception during tcp transport boot: " + err.Error())
	}

	raftconfig := inmemConfig()
	raftconfig.LogLevel = "info"
	raftconfig.LocalID = nodenameresolver.Resolve(string(transport.LocalAddr()))
	raftconfig.SnapshotThreshold = 512
	raftServer, err := raft.NewRaft(raftconfig, fsm.NewPrioritizedFSM(storage.NewPqStorage()), cacheStore, store, snapshotStore, transport)
	if err != nil {
		panic("exception during staging cluster boot: " + err.Error())
	}
	return raftServer, transport
}

func TestLeaderProxy_Process(t *testing.T) {
	type args struct {
		withMiddleware bool
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		wantStatusCode int
	}{
		{
			name: "check node removal through slave node with leader proxy",
			args: args{
				withMiddleware: true,
			},
			wantErr:        false,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "check node removal through slave node without leader proxy",
			args: args{
				withMiddleware: false,
			},
			wantErr:        false,
			wantStatusCode: http.StatusUnprocessableEntity,
		},
	}
	baseAPIport := 5565
	baseClusterPort := 5555
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodesTransport := []*raft.NetworkTransport{}
			clusters := []*raft.Raft{}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
			defer cancel()

			g, _ := errgroup.WithContext(ctx)

			for i := 0; i < 2; i++ {
				nodePort := strconv.Itoa(baseClusterPort + i + testID*2)
				apiPort := strconv.Itoa(baseAPIport + i + testID*2)

				cfg := config.Config{
					ClusterNodeHost: "localhost",
					ClusterNodePort: nodePort,
					APIPort:         apiPort,
				}

				node, nodeTransport := bootStagingCluster(cfg)
				defer func() {
					_ = node.Shutdown()
				}()
				nodesTransport = append(nodesTransport, nodeTransport)
				clusters = append(clusters, node)

				httpServer := echo.New()
				httpServer.HideBanner = true
				httpServer.HidePort = true
				httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}
				httpServer.Pre(nativemiddleware.RemoveTrailingSlash())

				// values below are hardcoded because we need to replace values on master node
				if tt.args.withMiddleware {
					httpServer.Use(NewLeaderProxy(node, config.Config{
						ClusterNodeHost: "localhost",
						ClusterNodePort: strconv.Itoa(baseClusterPort + testID*2),
						APIPort:         strconv.Itoa(baseAPIport + testID*2),
					}).Process)
				}

				m := &clustermanager.RaftClusterManager{}
				_ = m.BootRaftClusterManager(node, httpServer)
				g.Go(func() error {
					err := httpServer.Start(":" + apiPort)
					if err != nil {
						t.Fatal("http server failure: ", err.Error())
					}
					return err
				})

			}

			f := clusters[0].BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(nodesTransport[0].LocalAddr())),
					Address:  nodesTransport[0].LocalAddr(),
				},
				{
					Suffrage: raft.Staging,
					ID:       nodenameresolver.Resolve(string(nodesTransport[1].LocalAddr())),
					Address:  nodesTransport[1].LocalAddr(),
				},
			}})

			err := f.Error()
			if err != nil {
				t.Fatal(err.Error())
			}

			time.Sleep(time.Second * 2)

			// request delete through slave
			jsonInput := []byte("{\"node_addr\": \"" + string(nodesTransport[1].LocalAddr()) + "\"}")
			req, err := http.NewRequest(http.MethodDelete, "http://localhost:"+strconv.Itoa(baseAPIport+1+testID*2)+"/cluster", bytes.NewBuffer(jsonInput))
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			r, err := client.Do(req)
			if tt.wantErr {
				assert.Error(t, err)
			} else if assert.NoError(t, err) {
				defer r.Body.Close()
				assert.Equal(t, tt.wantStatusCode, r.StatusCode)
			}
		})
	}
}

func TestNewLeaderProxy(t *testing.T) {
	type args struct {
		cluster *raft.Raft
		config  config.Config
	}
	tests := []struct {
		name string
		args args
		want *LeaderProxy
	}{
		{
			name: "check leader proxy middleware constructor",
			args: args{
				cluster: &raft.Raft{},
				config:  config.Config{},
			},
			want: &LeaderProxy{
				cluster: &raft.Raft{},
				config:  config.Config{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NewLeaderProxy(tt.args.cluster, tt.args.config))
		})
	}
}
