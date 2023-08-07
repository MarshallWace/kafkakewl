# **kafkakewl** Introduction

**kafkakewl** is a set of services that help with manipulating kafka topics and kafka ACLs in kafka-clusters. It also gives information about the consumers' lag, status, and other metrics as well as various topic metrics.

It consists of the following components:
- **kafkakewl-api**: a service that exposes a REST API to manipulate kafkakewl topologies and get metrics about consumers/producers/topics.

- **kafkakewl-api-metrics**: another service with a REST API that gathers metrics about consumers/producers/topics and exposes them. The **kafkakewl-api** uses this service, it's typically not exposed for other public use.

- [**kafkakewl-ui**](https://github.com/MarshallWace/kafkakewl-ui/tree/legacy-main): a standard single page web-application that talks to the **kafkakewl-api** service, and shows the topologies, their topics and applications and some metrics.

# License

It's licensed under [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html#licenseText).

# Main Concepts

## Topology

The core concept of **kafkakewl** is the **topology**. A **topology** is a set of kafka topics, applications and their relationships (consume or produce). **Topologies** can be deployed to kafka-clusters and then their topics are created along with all the ACLs necessary for the applications to consume or produce the topics.

A very simple example topology:
```json
{
  "namespace": "projectx",
  "topics": {
    "topic1": {"name": "projectx.topic1"}
  },
  "applications": {
    "source-producer": { "user": "projectx-service-user" },
    "sink": { "user": "projectx-service-user", "type": { "consumerGroup": "projectx.sink" } }
  },
  "relationships": {
    "source-producer": { "produce": ["topic1"] },
    "sink": { "consume": ["topic1"] }
  }
}
```

It's effectively a higher level description of the topics and their consumers and producers, which allows **kafkakewl** to create/update the topics with the right configuration, create/update ACLs for users on kafka resources and to show us visually how the consumers and producers look as a graph.

There may be multiple, possibly very many different **topologies** deployed to different kafka-clusters. They can decide to expose some topics to certain other **topologies** for consumption/production, ultimately forming one or more bigger graphs of topics and applications.

This way of organizing topics and applications works well in an environment with multiple teams working on a set of topics and their consumers/producers and ultimately exposing some of those topics to other teams.

## Topic

A **kafkakewl** topic is just a kafka topic, with the exact same configuration options and more.

## Application

A **kafkakewl** application is something that's associated with exactly one service user and can consume or produce one ore more topics. Consumption must use the same consumer group.

It can correspond to exactly one deployed service, or can represent multiple services or maybe even just a part of a service.

## Relationship

Topics and applications can have relationships among them. The most common ones are **consume** and **produce**, but it's possible to have custom relationships.

It's also possible to have relationships between two topics and also two applications. It may sound weird, but sometimes useful for visualization purposes.

# Building, Deployment

## Prerequisites

**kafkakewl-api**, **kafkakewl-api-metrics**: jre 8 to run, jdk 8 to build (latest versions)

## Building from source to run locally (don't run it yet, you'll need to configure quite a few things first)

You'll need:
- jdk 8
- sbt

**kafkakewl-api**:

```bash
sbt kewl-api/stage
```

and then to launch:

```bash
kewl-api/target/universal/stage/bin/kewl-api
```

**kafkakewl-api-metrics**:

```bash
sbt kewl-api-metrics/stage
```

and then to launch:

```bash
kewl-api-metrics/target/universal/stage/bin/kewl-api-metrics
```

## Building the docker images

**kafkakewl-api**:

```bash
docker build -t kafkakewl-api -f kafkakewl-api.dockerfile .
```

**kafkakewl-api-metrics**:

```bash
docker build -t kafkakewl-api -f kafkakewl-api-metrics.dockerfile .
```

# **kafkakewl-api** Configuration

You need to [configure the **kafkakewl-api**](docs/configuration-kewl-api.md) service before starting.

If you run it with docker, to override the default `application.conf`, you'll need to mount your conf file to `/usr/src/app/.kafkakewl-api-overrides.conf`

# **kafkakewl-api-metrics** Configuration

You need to [configure the **kafkakewl-api-metrics**](docs/configuration-kewl-api-metrics.md) service before starting.

If you run it with docker, to override the default `application.conf`, you'll need to mount your conf file to `/usr/src/app/.kafkakewl-api-metrics-overrides.conf`

# **kafkakewl-api-metrics** prometheus metrics

The **kafkakewl-api-metrics** service exposes [prometheus metrics about topics and consumer groups](./docs/prometheus-metrics-kafkakewl-api-metrics.md). 

# Getting Started

So you configured your **kafkakewl-api** and **kafkakewl-api-metrics** services, you started them and they haven't failed. Now they are ready to be used, but in order to deploy topologies, a bit more setup is needed:

1. [add some permissions](docs/getting-started-permissions.md)
2. [register a kafka-cluster](docs/getting-started-kafkacluster.md)

Now let's [get started with some topology examples](docs/getting-started-topologies.md).

# REST API

The [detailed documentation of the REST API](docs/rest-api.md).
