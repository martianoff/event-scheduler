package channel

type Channel struct {
	ID          string      `json:"id" xml:"id"`
	Source      Source      `json:"source" xml:"source"`
	Destination Destination `json:"destination" xml:"destination"`
}

type Source struct {
	Driver string       `json:"driver" xml:"driver"`
	Config SourceConfig `json:"config" xml:"config"`
}

type Destination struct {
	Driver string            `json:"driver" xml:"driver"`
	Config DestinationConfig `json:"config" xml:"config"`
}

type SourceConfig interface{}
type DestinationConfig interface{}
