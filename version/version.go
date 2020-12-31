package version

import (
	"github.com/maksimru/event-scheduler/types"
	"runtime"
)

const (
	ProductName string = "event-scheduler"
)

// Revision that was compiled. This will be filled in by the compiler.
var Revision string

// BuildDate is when the binary was compiled.  This will be filled in by the
// compiler.
var BuildDate string

// Version number that is being run at the moment.  Version should use semver.
var Version string

func GetEventSchedulerVersion() types.VersionInfo {
	v := types.VersionInfo{
		Name:      ProductName,
		Revision:  Revision,
		BuildDate: BuildDate,
		Version:   Version,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}

	return v
}
