package pubsubconfig

type SourceConfig struct {
	ProjectID      string `json:"project_id" xml:"project_id" mapstructure:"project_id"`
	SubscriptionID string `json:"subscription_id" xml:"subscription_id" mapstructure:"subscription_id"`
	KeyFile        string `json:"key_file" xml:"key_file" mapstructure:"key_file"`
}
