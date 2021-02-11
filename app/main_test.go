package main

import (
	"context"
	joonix "github.com/joonix/log"
	"github.com/maksimru/event-scheduler/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func Test_setupLogger(t *testing.T) {
	type args struct {
		config config.Config
	}
	tests := []struct {
		name       string
		args       args
		wantLevel  string
		wantFormat log.Formatter
	}{
		{
			name: "Check logger options, json plus debug",
			args: args{
				config.Config{
					LogFormat: "json",
					LogLevel:  "debug",
				},
			},
			wantFormat: &log.JSONFormatter{},
			wantLevel:  "debug",
		},
		{
			name: "Check logger options, text plus trace",
			args: args{
				config.Config{
					LogFormat: "text",
					LogLevel:  "trace",
				},
			},
			wantFormat: &log.TextFormatter{},
			wantLevel:  "trace",
		},
		{
			name: "Check logger options, gcp plus info",
			args: args{
				config.Config{
					LogFormat: "gcp",
					LogLevel:  "info",
				},
			},
			wantFormat: joonix.NewFormatter(),
			wantLevel:  "info",
		},
		{
			name: "Check logger options, text plus fatal",
			args: args{
				config.Config{
					LogFormat: "text",
					LogLevel:  "fatal",
				},
			},
			wantFormat: &log.TextFormatter{},
			wantLevel:  "fatal",
		},
		{
			name: "Check logger options, text plus panic",
			args: args{
				config.Config{
					LogFormat: "text",
					LogLevel:  "panic",
				},
			},
			wantFormat: &log.TextFormatter{},
			wantLevel:  "panic",
		},
		{
			name: "Check logger options, text plus warning",
			args: args{
				config.Config{
					LogFormat: "text",
					LogLevel:  "warning",
				},
			},
			wantFormat: &log.TextFormatter{},
			wantLevel:  "warning",
		},
		{
			name: "Check logger options, text plus error",
			args: args{
				config.Config{
					LogFormat: "text",
					LogLevel:  "error",
				},
			},
			wantFormat: &log.TextFormatter{},
			wantLevel:  "error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupLogger(tt.args.config)
			assert.Equal(t, tt.wantLevel, log.GetLevel().String())
			assert.Equal(t, reflect.TypeOf(tt.wantFormat), reflect.TypeOf(log.StandardLogger().Formatter))
		})
	}
}

func getProjectPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	return dir
}

func TestMainFunc(t *testing.T) {
	dir := getProjectPath()
	tests := []struct {
		name             string
		expectedExitCode int
		expectedPanic    bool
		env              map[string]string
		removeDir        string
	}{
		{
			name:             "Test wrongly configured application launch",
			expectedExitCode: 1,
			expectedPanic:    false,
			env: map[string]string{
				"LISTENER_DRIVER":       "test",
				"PUBLISHER_DRIVER":      "test_w",
				"STORAGE_PATH":          dir + "/tests/tempStorage1",
				"CLUSTER_NODE_PORT":     "5555",
				"CLUSTER_INITIAL_NODES": "localhost:5555",
			},
			removeDir: dir + "/tests/tempStorage1",
		},
		{
			name:             "Test with unsupported drivers",
			expectedExitCode: 1,
			expectedPanic:    true,
			env: map[string]string{
				"LISTENER_DRIVER":           "amqp",
				"PUBLISHER_DRIVER":          "amqp",
				"PUBSUB_LISTENER_KEY_FILE":  dir + "/tests/pubsub_cred_mock.json",
				"PUBSUB_PUBLISHER_KEY_FILE": dir + "/tests/pubsub_cred_mock.json",
				"STORAGE_PATH":              dir + "/tests/tempStorage2",
				"CLUSTER_NODE_PORT":         "5556",
				"CLUSTER_INITIAL_NODES":     "localhost:5556",
			},
			removeDir: dir + "/tests/tempStorage2",
		},
		{
			name:             "Test correctly configured application launch",
			expectedExitCode: 0,
			expectedPanic:    false,
			env: map[string]string{
				"LISTENER_DRIVER":       "test",
				"PUBLISHER_DRIVER":      "test",
				"STORAGE_PATH":          dir + "/tests/tempStorage3",
				"CLUSTER_NODE_PORT":     "5557",
				"CLUSTER_INITIAL_NODES": "localhost:5557",
			},
			removeDir: dir + "/tests/tempStorage3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			// mock env vars
			_ = os.RemoveAll(tt.removeDir)
			for k, v := range tt.env {
				_ = os.Setenv(k, v)
			}
			if !tt.expectedPanic {
				assert.Equal(t, tt.expectedExitCode, app(ctx))
			} else {
				assert.Panics(t, func() {
					app(ctx)
				})
			}
		})
	}
}
