[![CircleCI](https://circleci.com/gh/maksimru/event-scheduler.svg?style=svg&circle-token=1d06eaf64a0b78e953c88044b894cea83ba3ee64)](https://circleci.com/gh/maksimru/event-scheduler)
[![codecov](https://codecov.io/gh/maksimru/event-scheduler/branch/master/graph/badge.svg?token=7mfqEvwubF)](https://codecov.io/gh/maksimru/event-scheduler)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/maksimru/event-scheduler)](https://pkg.go.dev/github.com/maksimru/event-scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/maksimru/event-scheduler)](https://goreportcard.com/report/github.com/maksimru/event-scheduler)

# What is it?

Event-scheduler is an application designed to receive messages from the source queue and release them to the target queue at requested time, specified in message attribute

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
| LOG_FORMAT            | string    | json             | log format output: json or text           |
| LOG_LEVEL             | string     | info              | log verbosity            |
| LISTENER_DRIVER             | string     | pubsub              | source (scheduled) messages queue driver          |
| PUBSUB_LISTENER_PROJECT_ID             | string     |               | source queue pubsub project id            |
| PUBSUB_LISTENER_SUBSCRIPTION_ID             | string     |               | source queue pubsub subscription id            |
| PUBSUB_LISTENER_KEY_FILE             | string     |               | path to pubsub service account access key file for source queue           |
| PUBLISHER_DRIVER             | string     | pubsub              | target messages queue driver            |
| PUBSUB_PUBLISHER_PROJECT_ID             | string     |               | target queue pubsub project id            |
| PUBSUB_PUBLISHER_TOPIC_ID             | string     |               | target queue pubsub topic id            |
| PUBSUB_PUBLISHER_KEY_FILE             | string     |               | path to pubsub service account access key file for target queue            |

## Execution

```bash
go run app/main.go
```