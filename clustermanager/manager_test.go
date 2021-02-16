package clustermanager

import (
	"github.com/go-playground/validator"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/httpvalidator"
	"github.com/maksimru/event-scheduler/nodenameresolver"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRaftClusterManager_BootRaftClusterManager(t *testing.T) {
	type args struct {
		cluster    *raft.Raft
		httpServer *echo.Echo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Check cluster manager boot",
			args: args{
				cluster:    &raft.Raft{},
				httpServer: echo.New(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &RaftClusterManager{}
			if err := m.BootRaftClusterManager(tt.args.cluster, tt.args.httpServer); (err != nil) != tt.wantErr {
				t.Errorf("BootRaftClusterManager() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRaftClusterManager_AddNode(t *testing.T) {
	type fields struct {
		cluster    *raft.Raft
		httpServer *echo.Echo
	}
	type args struct {
		jsonInput string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check add node API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "",
			},
			wantErr: false,
		},
		{
			name: "Check add node API with invalid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "{\"foo\": \"bar\"}",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			clusterManagers := []*RaftClusterManager{}
			nodesTransport := []*raft.InmemTransport{}

			for i := 0; i < 2; i++ {
				node, nodeTransport := bootStagingCluster()
				defer func() {
					_ = node.Shutdown()
				}()
				m := &RaftClusterManager{
					cluster:    node,
					httpServer: tt.fields.httpServer,
				}
				m.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}
				f := m.cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
					{
						Suffrage: raft.Voter,
						ID:       nodenameresolver.Resolve(string(nodeTransport.LocalAddr())),
						Address:  nodeTransport.LocalAddr(),
					},
				}})
				err := f.Error()
				if err != nil {
					t.Fatal(err.Error())
				}
				clusterManagers = append(clusterManagers, m)
				nodesTransport = append(nodesTransport, nodeTransport)
			}

			// wire servers together
			nodesTransport[0].Connect(nodesTransport[1].LocalAddr(), nodesTransport[1])
			nodesTransport[1].Connect(nodesTransport[0].LocalAddr(), nodesTransport[0])

			time.Sleep(time.Second * 1)

			if tt.args.jsonInput == "" {
				tt.args.jsonInput = "{\"node_addr\": \"" + string(nodesTransport[1].LocalAddr()) + "\"}"
			}

			// mock http request
			req := httptest.NewRequest(http.MethodGet, "/cluster", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := clusterManagers[0].httpServer.NewContext(req, rec)

			r := clusterManagers[0].AddNode(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				time.Sleep(time.Second * 1)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

func TestRaftClusterManager_DeleteNode(t *testing.T) {
	type fields struct {
		cluster    *raft.Raft
		httpServer *echo.Echo
	}
	type args struct {
		jsonInput string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check delete node API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "",
			},
			wantErr: false,
		},
		{
			name: "Check delete node API with invalid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "{\"foo\": \"bar\"}",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterManagers := []*RaftClusterManager{}
			nodesTransport := []*raft.InmemTransport{}

			for i := 0; i < 2; i++ {
				node, nodeTransport := bootStagingCluster()
				defer func() {
					_ = node.Shutdown()
				}()
				m := &RaftClusterManager{
					cluster:    node,
					httpServer: tt.fields.httpServer,
				}
				m.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}

				clusterManagers = append(clusterManagers, m)
				nodesTransport = append(nodesTransport, nodeTransport)
			}

			// wire servers together
			nodesTransport[0].Connect(nodesTransport[1].LocalAddr(), nodesTransport[1])
			nodesTransport[1].Connect(nodesTransport[0].LocalAddr(), nodesTransport[0])

			f := clusterManagers[0].cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(nodesTransport[0].LocalAddr())),
					Address:  nodesTransport[0].LocalAddr(),
				},
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(nodesTransport[1].LocalAddr())),
					Address:  nodesTransport[1].LocalAddr(),
				},
			}})

			err := f.Error()
			if err != nil {
				t.Fatal(err.Error())
			}

			time.Sleep(time.Second * 1)

			if tt.args.jsonInput == "" {
				tt.args.jsonInput = "{\"node_addr\": \"" + string(nodesTransport[1].LocalAddr()) + "\"}"
			}

			// mock http request
			req := httptest.NewRequest(http.MethodDelete, "/cluster", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := clusterManagers[0].httpServer.NewContext(req, rec)

			r := clusterManagers[0].DeleteNode(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				time.Sleep(time.Second * 1)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

func inmemConfig() *raft.Config {
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	return conf
}

func bootStagingCluster() (*raft.Raft, *raft.InmemTransport) {
	store := raft.NewInmemStore()
	cacheStore, _ := raft.NewLogCache(128, store)
	snapshotStore := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport("")
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

func TestRaftClusterManager_Status(t *testing.T) {
	type fields struct {
		httpServer *echo.Echo
	}
	type args struct {
		jsonInput string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check status endpoint",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cluster, nodeTransport := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			m := &RaftClusterManager{
				cluster:    cluster,
				httpServer: tt.fields.httpServer,
			}

			f := m.cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(nodeTransport.LocalAddr())),
					Address:  nodeTransport.LocalAddr(),
				},
			}})

			err := f.Error()
			if err != nil {
				t.Fatal(err.Error())
			}

			// mock http request
			req := httptest.NewRequest(http.MethodGet, "/cluster", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := m.httpServer.NewContext(req, rec)

			if assert.NoError(t, m.Status(c)) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}
