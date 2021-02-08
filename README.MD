[![CircleCI](https://circleci.com/gh/maksimru/event-scheduler.svg?style=svg&circle-token=1d06eaf64a0b78e953c88044b894cea83ba3ee64)](https://circleci.com/gh/maksimru/event-scheduler)
[![codecov](https://codecov.io/gh/maksimru/event-scheduler/branch/master/graph/badge.svg?token=7mfqEvwubF)](https://codecov.io/gh/maksimru/event-scheduler)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/maksimru/event-scheduler)](https://pkg.go.dev/github.com/maksimru/event-scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/maksimru/event-scheduler)](https://goreportcard.com/report/github.com/maksimru/event-scheduler)

# What is it?

Event-scheduler is a high available strongly consistent (powered by [raft](https://en.wikipedia.org/wiki/Raft_(algorithm))) application designed to receive messages from the source queue and release them to the target queue at requested time, specified in message attribute

## Currently supported queue drivers

1) Google Pubsub

## Pubsub queue configuration

1) Add available_at (timestamp in seconds) attribute to your pubsub messages to tell the scheduler when you want them to be released to the target queue
2) Add Pubsub subscription with a filter "attributes:available_at" and use it as source queue
3) Create target topic (you can use same topic as source topic but make sure your application's subscription has the filter "NOT attributes:available_at"). So event-scheduler will consume scheduled only messages, and your app will consume real-time messages only

## Scheduler configuration

Event scheduler can be configured via env vars:

| Env var name | Type | Default Value | Description |
|--------------|------|---------------|-------------|
| LOG_FORMAT            | string    | text             | log format output: json, text, gcp           |
| LOG_LEVEL             | string     | info              | log verbosity            |
| LISTENER_DRIVER             | string     | pubsub              | source (scheduled) messages queue driver          |
| PUBSUB_LISTENER_PROJECT_ID             | string     |               | source queue pubsub project id            |
| PUBSUB_LISTENER_SUBSCRIPTION_ID             | string     |               | source queue pubsub subscription id            |
| PUBSUB_LISTENER_KEY_FILE             | string     |               | path to pubsub service account access key file for source queue           |
| PUBLISHER_DRIVER             | string     | pubsub              | target messages queue driver            |
| PUBSUB_PUBLISHER_PROJECT_ID             | string     |               | target queue pubsub project id            |
| PUBSUB_PUBLISHER_TOPIC_ID             | string     |               | target queue pubsub topic id            |
| PUBSUB_PUBLISHER_KEY_FILE             | string     |               | path to pubsub service account access key file for target queue            |
| STORAGE_PATH             | string     | storage              | path for persistent data storage            |
| CLUSTER_NODE_HOST             | string     | localhost              | node host, should be accessible from other cluster nodes            |
| CLUSTER_NODE_PORT             | string     | 5559              | node port for interaction with other cluster nodes           |
| CLUSTER_INITIAL_NODES             | string     | localhost:5559              | comma separated list of cluster nodes           |
| CLUSTER_INITIAL_LEADER             | string     | localhost:5559              | cluster leader address           |

## Execution

```bash
go run app/main.go
```

## Tests

```bash
go test -covermode=atomic ./...
```

## Run with Kubernetes

```bash
# replace serviceaccount.json to the path to your service account key file with PubsubPermissions
kubectl create secret generic pubsubserviceaccount --from-file key.json=serviceaccount.json
# feel free to adjust env vars inside deployment manifest
kubectl apply -f ./event-scheduler/kubernetes/deployment
```

## Future / Roadmap (Unordered)

- Message compression
- Multi-channel support
- ~~High availability~~
- ~~Persistence storage~~
- Prometheus metrics
- ~~Test cases~~
- Horizontal scaling / Sharding
- Helm package
- ~~Docker build~~
- ~~Sample kubernetes deployment~~