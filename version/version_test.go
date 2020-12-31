package version

import (
	"github.com/maksimru/event-scheduler/types"
	"github.com/stretchr/testify/assert"
	"reflect"
	"runtime"
	"testing"
)

func TestGetEventSchedulerVersion(t *testing.T) {
	tests := []struct {
		name string
		want types.VersionInfo
	}{
		{
			name: "Version info",
			want: types.VersionInfo{
				Name:      ProductName,
				Revision:  Revision,
				BuildDate: BuildDate,
				Version:   Version,
				GoVersion: runtime.Version(),
				OS:        runtime.GOOS,
				Arch:      runtime.GOARCH,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetEventSchedulerVersion(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
