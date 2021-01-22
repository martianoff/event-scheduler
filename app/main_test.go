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
		name         string
		expectedExit int
		env          map[string]string
	}{
		{
			name:         "Test wrongly configured application launch",
			expectedExit: 1,
			env: map[string]string{
				"LISTENER_DRIVER":           "pubsub",
				"PUBLISHER_DRIVER":          "pubsub",
				"PUBSUB_LISTENER_KEY_FILE":  dir + "/tests/pubsub_cred_mock.json",
				"PUBSUB_PUBLISHER_KEY_FILE": dir + "/tests/pubsub_cred_mock.json",
			},
		},
		{
			name:         "Test correctly configured application launch",
			expectedExit: 0,
			env: map[string]string{
				"LISTENER_DRIVER":  "test",
				"PUBLISHER_DRIVER": "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*1)
			defer cancel()
			// mock env vars
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			assert.Equal(t, tt.expectedExit, app(ctx))
		})
	}
}
