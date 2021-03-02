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

type Updated func(c Channel)
type Deleted func(c Channel)
type Added func(c Channel)

type EventHandler struct {
	channelUpdatedCallback Updated
	channelDeletedCallback Deleted
	channelAddedCallback   Added
}

func NewEventHandler(channelUpdatedCallback Updated, channelDeletedCallback Deleted, channelAddedCallback Added) *EventHandler {
	return &EventHandler{channelUpdatedCallback: channelUpdatedCallback, channelDeletedCallback: channelDeletedCallback, channelAddedCallback: channelAddedCallback}
}

func (h *EventHandler) OnAdded(c Channel) {
	go func() {
		h.channelAddedCallback(c)
	}()
}

func (h *EventHandler) OnUpdated(c Channel) {
	go func() {
		h.channelUpdatedCallback(c)
	}()
}

func (h *EventHandler) OnDeleted(c Channel) {
	go func() {
		h.channelDeletedCallback(c)
	}()
}
