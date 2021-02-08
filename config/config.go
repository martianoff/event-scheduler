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
	StoragePath                  string `env:"STORAGE_PATH" envDefault:"storage"`
	ClusterNodeHost              string `env:"CLUSTER_NODE_HOST" envDefault:"localhost"`
	ClusterNodePort              string `env:"CLUSTER_NODE_PORT" envDefault:"5559"`
	ClusterInitialNodes          string `env:"CLUSTER_INITIAL_NODES" envDefault:"localhost:5559"`
	ClusterInitialLeader         string `env:"CLUSTER_INITIAL_LEADER" envDefault:"localhost:5559"`
}
