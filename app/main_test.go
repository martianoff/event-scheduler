package main

import (
	joonix "github.com/joonix/log"
	"github.com/maksimru/event-scheduler/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupLogger(tt.args.config)
			assert.Equal(t, tt.wantLevel, log.GetLevel().String())
			assert.Equal(t, reflect.TypeOf(tt.wantFormat), reflect.TypeOf(log.StandardLogger().Formatter))
		})
	}
}
