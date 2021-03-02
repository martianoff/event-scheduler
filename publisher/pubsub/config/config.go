package pubsubconfig

type DestinationConfig struct {
	ProjectID string `json:"project_id" xml:"project_id" mapstructure:"project_id"`
	TopicID   string `json:"topic_id" xml:"topic_id" mapstructure:"topic_id"`
	KeyFile   string `json:"key_file" xml:"key_file" mapstructure:"key_file"`
}
