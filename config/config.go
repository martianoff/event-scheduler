package config

type Config struct {
	LogFormat                    string `env:"LOG_FORMAT" envDefault:"text"`
	LogLevel                     string `env:"LOG_LEVEL" envDefault:"info"`
	ListenerDriver               string `env:"LISTENER_DRIVER" envDefault:"pubsub"`
	PubsubListenerProjectID      string `env:"PUBSUB_LISTENER_PROJECT_ID"`
	PubsubListenerSubscriptionID string `env:"PUBSUB_LISTENER_SUBSCRIPTION_ID"`
	PubsubListenerKeyFile        string `env:"PUBSUB_LISTENER_KEY_FILE"`
	PublisherDriver              string `env:"PUBLISHER_DRIVER" envDefault:"pubsub"`
	PubsubPublisherProjectID     string `env:"PUBSUB_PUBLISHER_PROJECT_ID"`
	PubsubPublisherTopicID       string `env:"PUBSUB_PUBLISHER_TOPIC_ID"`
	PubsubPublisherKeyFile       string `env:"PUBSUB_PUBLISHER_KEY_FILE"`
}
