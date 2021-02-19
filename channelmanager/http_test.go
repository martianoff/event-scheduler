package channelmanager

import (
	"github.com/go-playground/validator/v10"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/maksimru/event-scheduler/channel"
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

func TestSchedulerChannelManagerServer_BootChannelManagerServer(t *testing.T) {
	type args struct {
		manager    ChannelManager
		httpServer *echo.Echo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    SchedulerChannelManagerServer
	}{
		{
			name: "Check channel manager server boot",
			args: args{
				manager: &SchedulerChannelManager{
					cluster: &raft.Raft{},
					storage: storage.NewPqStorage(),
				},
				httpServer: echo.New(),
			},
			want: SchedulerChannelManagerServer{
				manager: &SchedulerChannelManager{
					cluster: &raft.Raft{},
					storage: storage.NewPqStorage(),
				},
				httpServer: echo.New(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &SchedulerChannelManagerServer{}
			err := ms.BootChannelManagerServer(tt.args.manager, tt.args.httpServer)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, ms.manager, tt.want.manager)
		})
	}
}

func TestSchedulerChannelManagerServer_AddChannel(t *testing.T) {
	type fields struct {
		manager    ChannelManager
		httpServer *echo.Echo
	}
	type args struct {
		jsonInput string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantErr        bool
		wantStatusCode int
	}{
		{
			name: "Check add channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				jsonInput: "{\"source\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"subscription_id\":\"test_subscription\",\"key_file\":\"test_key_file\"}},\"destination\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"topic_id\":\"test_topic\",\"key_file\":\"test_key_file\"}}}",
			},
			wantErr:        false,
			wantStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			ms := &SchedulerChannelManagerServer{
				manager:    m,
				httpServer: tt.fields.httpServer,
			}

			ms.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}

			// mock http request
			req := httptest.NewRequest(http.MethodPost, "/channels", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := ms.httpServer.NewContext(req, rec)

			r := ms.AddChannel(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				assert.Equal(t, tt.wantStatusCode, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}

		})
	}
}

func TestSchedulerChannelManagerServer_DeleteChannel(t *testing.T) {
	type fields struct {
		manager    ChannelManager
		httpServer *echo.Echo
	}
	type args struct {
		channelId string
		jsonInput string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		channels       []channel.Channel
		wantErr        bool
		wantStatusCode int
	}{
		{
			name: "Check delete existing channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				channelId: "ch1",
				jsonInput: "{}",
			},
			channels: []channel.Channel{
				{
					ID:          "ch1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr:        false,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "Check delete non-existing channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				channelId: "ch1",
				jsonInput: "{}",
			},
			channels:       []channel.Channel{},
			wantErr:        false,
			wantStatusCode: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			ms := &SchedulerChannelManagerServer{
				manager:    m,
				httpServer: tt.fields.httpServer,
			}

			ms.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}

			// mock http request
			req := httptest.NewRequest(http.MethodDelete, "/channels/:id", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := ms.httpServer.NewContext(req, rec)
			c.SetParamNames("id")
			c.SetParamValues(tt.args.channelId)

			r := ms.DeleteChannel(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				assert.Equal(t, tt.wantStatusCode, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

func TestSchedulerChannelManagerServer_UpdateChannel(t *testing.T) {
	type fields struct {
		manager    ChannelManager
		httpServer *echo.Echo
	}
	type args struct {
		channelId string
		jsonInput string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		channels       []channel.Channel
		wantErr        bool
		wantStatusCode int
	}{
		{
			name: "Check update existing channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				channelId: "ch1",
				jsonInput: "{\"source\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"subscription_id\":\"test_subscription\",\"key_file\":\"test_key_file\"}},\"destination\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"topic_id\":\"test_topic\",\"key_file\":\"test_key_file\"}}}",
			},
			channels: []channel.Channel{
				{
					ID:          "ch1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr:        false,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "Check update non-existing channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			args: args{
				channelId: "ch1",
				jsonInput: "{\"source\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"subscription_id\":\"test_subscription\",\"key_file\":\"test_key_file\"}},\"destination\":{\"driver\":\"pubsub\",\"config\":{\"project_id\":\"test_project\",\"topic_id\":\"test_topic\",\"key_file\":\"test_key_file\"}}}",
			},
			channels:       []channel.Channel{},
			wantErr:        false,
			wantStatusCode: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			ms := &SchedulerChannelManagerServer{
				manager:    m,
				httpServer: tt.fields.httpServer,
			}

			ms.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}

			// mock http request
			req := httptest.NewRequest(http.MethodPatch, "/channels/:id", strings.NewReader(tt.args.jsonInput))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := ms.httpServer.NewContext(req, rec)
			c.SetParamNames("id")
			c.SetParamValues(tt.args.channelId)

			r := ms.UpdateChannel(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				assert.Equal(t, tt.wantStatusCode, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

func TestSchedulerChannelManagerServer_ListChannels(t *testing.T) {
	type fields struct {
		manager    ChannelManager
		httpServer *echo.Echo
	}
	tests := []struct {
		name           string
		fields         fields
		channels       []channel.Channel
		wantErr        bool
		wantStatusCode int
	}{
		{
			name: "Check list channel API with valid input",
			fields: fields{
				httpServer: echo.New(),
			},
			channels: []channel.Channel{
				{
					ID:          "ch1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr:        false,
			wantStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			ms := &SchedulerChannelManagerServer{
				manager:    m,
				httpServer: tt.fields.httpServer,
			}

			ms.httpServer.Validator = &httpvalidator.HttpValidator{Validator: validator.New()}

			// mock http request
			req := httptest.NewRequest(http.MethodGet, "/channels", nil)
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := ms.httpServer.NewContext(req, rec)

			r := ms.ListChannels(c)
			if tt.wantErr {
				assert.Error(t, r)
			} else if assert.NoError(t, r) {
				assert.Equal(t, tt.wantStatusCode, rec.Code)
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}
