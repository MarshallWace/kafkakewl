/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.topology._

trait TestTopologiesToDeploy {
  this: TestTopologiesToDeployCommon =>

  def topologyToDeploySourceSink(project: String = "test", withTransactionalId: Boolean = true, developersAccess: DevelopersAccess = DevelopersAccess.TopicReadOnly, deployWithAuthorizationCode: Option[Boolean] = None) =
    TopologyToDeploy(
      Namespace(project),
      developersAccess = developersAccess,
      deployWithAuthorizationCode = deployWithAuthorizationCode,
      topics = Map("topic" -> TopologyToDeploy.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project"),
        s"sink" -> TopologyToDeploy.Application(s"service-$project").makeSimple(Some(s"$project.sink"), if (withTransactionalId) Some(s"$project.sink") else None)
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        toDeployRelationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"topic", None))))
      )
    )

  def topologyToDeployKafkaStreams(project: String = "test"): TopologyToDeploy = {
    TopologyToDeploy(
      Namespace(project),
      topics = Map(
        "topic-input" -> TopologyToDeploy.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-intermediate" -> TopologyToDeploy.Topic(s"$project.topic-intermediate", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> TopologyToDeploy.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project"),
        s"processor" -> TopologyToDeploy.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-input", None)))),
        toDeployRelationshipFrom(s"processor", (RelationshipType.Consume(), Seq((s"topic-input", None)))),
        toDeployRelationshipFrom(s"processor", (RelationshipType.Consume(), Seq((s"topic-intermediate", None))), (RelationshipType.Produce(), Seq((s"topic-intermediate", None)))),
        toDeployRelationshipFrom(s"processor", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )
  }

  def topologyToDeployKafkaStreamsWithMultipleApplications(project: String = "test"): TopologyToDeploy = {
    TopologyToDeploy(
      Namespace(project),
      topics = Map(
        "topic-input" -> TopologyToDeploy.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> TopologyToDeploy.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project"),
        s"processor-Parser" -> TopologyToDeploy.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor"),
        s"processor-Enricher" -> TopologyToDeploy.Application(s"service-$project").makeKafkaStreams(s"$project.__kstreams__.Processor")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-input", None)))),
        toDeployRelationshipFrom(s"processor-Parser", (RelationshipType.Consume(), Seq((s"topic-input", None))), (RelationshipType.Produce(), Seq((s"processor-Enricher", None)))),
        toDeployRelationshipFrom(s"processor-Enricher", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )
  }

  def topologyToDeployConnector(project: String = "test") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map("topic-output" -> TopologyToDeploy.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project").makeConnector(s"some-connect")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )

  def topologyToDeployConnectReplicator(project: String = "test") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map(
        "topic-input" -> TopologyToDeploy.Topic(s"$project.topic-input", config = Map("retention.ms" -> "31536000000")),
        "topic-output" -> TopologyToDeploy.Topic(s"$project.topic-output", config = Map("retention.ms" -> "31536000000"))
      ).toMapByTopicId,
      applications = Map(
        s"replicator" -> TopologyToDeploy.Application(s"service-$project").makeConnectReplicator(s"connect-replicator")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"replicator", (RelationshipType.Consume(), Seq((s"topic-input", None)))),
        toDeployRelationshipFrom(s"replicator", (RelationshipType.Produce(), Seq((s"topic-output", None))))
      )
    )

  def topologyToDeploySharedConsumableTopic(project: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map("topic-to-consume" -> TopologyToDeploy.Topic(s"$project.topic-to-consume", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any()))).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic-to-consume", None))))
      )
    )

  def topologyToDeployConsumingSharedTopic(project: String = "test", sharedProject: String = "shared", user: Option[String] = None) =
    TopologyToDeploy(
      Namespace(project),
      applications = Map(
        s"sink" -> TopologyToDeploy.Application(user.getOrElse(s"service-$project")).makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"$sharedProject.topic-to-consume", None))))
      )
    )

  def topologyToDeploySharedProducableTopic(project: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map("topic-to-produce" -> TopologyToDeploy.Topic(s"$project.topic-to-produce", config = Map("retention.ms" -> "31536000000"), otherProducerNamespaces = Seq(FlexibleName.Any()))).toMapByTopicId,
      applications = Map(
        s"sink" -> TopologyToDeploy.Application(s"service-$project").makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"topic-to-produce", None))))
      )
    )

  def topologyToDeployProducingSharedTopic(project: String = "test", sharedProject: String = "shared", user: Option[String] = None) =
    TopologyToDeploy(
      Namespace(project),
      applications = Map(
        s"source" -> TopologyToDeploy.Application(user.getOrElse(s"service-$project"))
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"$sharedProject.topic-to-produce", None))))
      )
    )

  def topologyToDeploySharedConsumingApplication(project: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      applications = Map(
        s"consuming-sink" -> TopologyToDeploy.Application(s"service-$project", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some(s"$project.consuming-sink"))
      ).toMapByApplicationId
    )

  def topologyToDeployWithTopicConsumedBySharedConsumingApplication(project: String = "test", sharedProject: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map("topic" -> TopologyToDeploy.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"source" -> TopologyToDeploy.Application(s"service-$project")
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        toDeployRelationshipFrom(s"$sharedProject.consuming-sink", (RelationshipType.Consume(), Seq((s"topic", None))))
      )
    )

  def topologyToDeploySharedProducingApplication(project: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      applications = Map(
        s"producing-source" -> TopologyToDeploy.Application(s"service-$project", otherProducableNamespaces = Seq(FlexibleName.Any()))
      ).toMapByApplicationId
    )

  def topologyToDeployWithTopicProducedBySharedProducingApplication(project: String = "test", sharedProject: String = "shared") =
    TopologyToDeploy(
      Namespace(project),
      topics = Map("topic" -> TopologyToDeploy.Topic(s"$project.topic", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
      applications = Map(
        s"sink" -> TopologyToDeploy.Application(s"service-$project").makeSimple(Some(s"$project.sink"))
      ).toMapByApplicationId,
      relationships = Map(
        toDeployRelationshipFrom(s"$sharedProject.producing-source", (RelationshipType.Produce(), Seq((s"topic", None)))),
        toDeployRelationshipFrom(s"sink", (RelationshipType.Consume(), Seq((s"topic", None))))
      )
    )
}
