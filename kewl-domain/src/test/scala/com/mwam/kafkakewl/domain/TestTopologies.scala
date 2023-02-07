/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.topology._

trait TestTopologies {
  this: TestTopologiesCommon =>

  def topologySourceSink(project: String = "test", withTransactionalId: Boolean = true) =
    Topology(
      Namespace(project),
      topics = Map("topic" -> Topology.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project"),
        s"sink" -> Topology.Application(s"service-$project").makeSimple(Some(s"$project.sink"), if (withTransactionalId) Some(s"$project.sink") else None)
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        relationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"topic", None))))
      )
    )

  def topologyKafkaStreams(project: String = "test"): Topology = {
    Topology(
      Namespace(project),
      topics = Map(
        "topic-input" -> Topology.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-intermediate" -> Topology.Topic(s"$project.topic-intermediate", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> Topology.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project"),
        s"processor" -> Topology.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-input", None)))),
        relationshipFrom(s"processor", (RelationshipType.Consume(), Seq((s"topic-input", None)))),
        relationshipFrom(s"processor", (RelationshipType.Consume(), Seq((s"topic-intermediate", None))), (RelationshipType.Produce(), Seq((s"topic-intermediate", None)))),
        relationshipFrom(s"processor", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )
  }

  def topologyKafkaStreamsWithMultipleApplications(project: String = "test"): Topology = {
    Topology(
      Namespace(project),
      topics = Map(
        "topic-input" -> Topology.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> Topology.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project"),
        s"processor-Parser" -> Topology.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor"),
        s"processor-Enricher" -> Topology.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-input", None)))),
        relationshipFrom(s"processor-Parser", (RelationshipType.Consume(), Seq((s"topic-input", None)))),
        relationshipFrom(s"processor-Parser", (RelationshipType.Produce(), Seq((s"processor-Enricher", None)))),
        relationshipFrom(s"processor-Enricher", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )
  }

  def topologyConnector(project: String = "test") =
    Topology(
      Namespace(project),
      topics = Map("topic-output" -> Topology.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project").makeConnector(s"some-connect")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )

  def topologyConnectReplicator(project: String = "test") =
    Topology(
      Namespace(project),
      topics = Map(
        "topic-input" -> Topology.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> Topology.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"replicator" -> Topology.Application(s"service-$project").makeConnectReplicator(s"connect-replicator")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"replicator", (RelationshipType.Consume(), Seq((s"topic-input", None)))),
        relationshipFrom(s"replicator", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )

  def topologySharedConsumableTopic(project: String = "shared") =
    Topology(
      Namespace(project),
      topics = Map("topic-to-consume" -> Topology.Topic(s"$project.topic-to-consume", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any()))).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-to-consume", None))))
      )
    )

  def topologyConsumingSharedTopic(project: String = "test", sharedProject: String = "shared") =
    Topology(
      Namespace(project),
      applications = Map(
        s"sink" -> Topology.Application(s"service-$project").makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"$sharedProject.topic-to-consume", None))))
      )
    )

  def topologySharedProducableTopic(project: String = "shared") =
    Topology(
      Namespace(project),
      topics = Map("topic-to-produce" -> Topology.Topic(s"$project.topic-to-produce", config = Map("retention.ms" -> "31536000000"), otherProducerNamespaces = Seq(FlexibleName.Any()))).toMapByTopicId,
      applications = Map(
        s"sink" -> Topology.Application(s"service-$project").makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"topic-to-produce", None))))
      )
    )

  def topologyProducingSharedTopic(project: String = "test", sharedProject: String = "shared") =
    Topology(
      Namespace(project),
      applications = Map(
        s"source" -> Topology.Application(s"service-$project")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"$sharedProject.topic-to-produce", None))))
      )
    )

  def topologySharedConsumingApplication(project: String = "shared") =
    Topology(
      Namespace(project),
      applications = Map(
        s"consuming-sink" -> Topology.Application(s"service-$project", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some(s"$project.consuming-sink"))
      ).toMapByApplicationId
    )

  def topologyWithTopicConsumedBySharedConsumingApplication(project: String = "test", sharedProject: String = "shared") =
    Topology(
      Namespace(project),
      topics = Map("topic" -> Topology.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-$project")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        relationshipFrom(s"$sharedProject.consuming-sink", (RelationshipType.Consume(), Seq((s"topic", None))))
      )
    )

  def topologySharedProducingApplication(project: String = "shared") =
    Topology(
      Namespace(project),
      applications = Map(
        s"producing-source" -> Topology.Application(s"service-$project", otherProducableNamespaces = Seq(FlexibleName.Any()))
      ).toMapByApplicationId
    )

  def topologyWithTopicProducedBySharedProducingApplication(project: String = "test", sharedProject: String = "shared") =
    Topology(
      Namespace(project),
      topics = Map("topic" -> Topology.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"sink" -> Topology.Application(s"service-$project").makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(s"$sharedProject.producing-source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        relationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"$project.topic", None))))
      )
    )
}
