/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import cats.syntax.either._
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.deploy.TopicPartitionPosition
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{CommandError, FlexibleName, TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.WordSpec

class ResetApplicationOffsetsSpec extends WordSpec
  with ResetApplicationOffsetsCommon
  with ResetCommon
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon
  with TopologyToDeployOperations
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  "default ResetApplicationOptions.ApplicationTopics" when {
    "the single input topic has only a single partition" should {
      "reset all consume to the default position" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeploySourceSink()

        val actual = calculateTopicPartitionsToReset(
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = Map("test.topic" -> Iterable((0, TopicPartitionPosition.Beginning())))

        assert(actual == expected)
      }
    }

    "the single input topic has more partitions" should {
      "reset all consume to the default position" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeploySourceSink().updateTopic("topic", t => t.copy(partitions = 3))

        val actual = calculateTopicPartitionsToReset(
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = Map(
          "test.topic" -> Iterable(
            (0, TopicPartitionPosition.Beginning()),
            (1, TopicPartitionPosition.Beginning()),
            (2, TopicPartitionPosition.Beginning())
          )
        )

        assert(actual == expected)
      }
    }

    "the topology has external inputs too" should {
      "reset all consume to the default position" in {
        val sharedTopologyId = TopologyEntityId("shared")
        val sharedTopology = topologyToDeploySharedConsumableTopic()
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = (topologyToDeployConsumingSharedTopic() + (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", partitions = 3)))
            .withRelationship(
              toDeployRelationshipFrom("sink", (RelationshipType.Consume(), Seq(("shared.topic-to-consume", None), ("topic", None))))
            )

        val actual = calculateTopicPartitionsToReset(
          Map(sharedTopologyId -> sharedTopology, topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = Map(
          "test.topic" -> Iterable(
            (0, TopicPartitionPosition.Beginning()),
            (1, TopicPartitionPosition.Beginning()),
            (2, TopicPartitionPosition.Beginning())
          ),
          "shared.topic-to-consume" -> Iterable(
            (0, TopicPartitionPosition.Beginning())
          )
        )

        assert(actual == expected)
      }
    }

    "the topology has external inputs too but one relationship's reset is to be ignored" should {
      "reset all consume to the default position" in {
        val sharedTopologyId = TopologyEntityId("shared")
        val sharedTopology = topologyToDeploySharedConsumableTopic()
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = (topologyToDeployConsumingSharedTopic() + (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", partitions = 3)))
          .withRelationship(
            toDeployRelationshipFrom("sink", (RelationshipType.Consume(), Seq(("shared.topic-to-consume", None), ("topic", Some(TopologyToDeploy.RelationshipProperties(reset = Some(ResetMode.Ignore)))))))
          )

        val actual = calculateTopicPartitionsToReset(
          Map(sharedTopologyId -> sharedTopology, topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = Map(
          "shared.topic-to-consume" -> Iterable(
            (0, TopicPartitionPosition.Beginning())
          )
        )

        assert(actual == expected)
      }
    }

    "the topology has external inputs too but one relationship's reset is End" should {
      "reset all consume to the default position" in {
        val sharedTopologyId = TopologyEntityId("shared")
        val sharedTopology = topologyToDeploySharedConsumableTopic()
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = (topologyToDeployConsumingSharedTopic() + (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", partitions = 3)))
          .withRelationship(toDeployRelationshipFrom("sink", (RelationshipType.Consume(), Seq(("shared.topic-to-consume", None), ("topic", Some(TopologyToDeploy.RelationshipProperties(reset = Some(ResetMode.End))))))))

        val actual = calculateTopicPartitionsToReset(
          Map(sharedTopologyId -> sharedTopology, topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = Map(
          "test.topic" -> Iterable(
            (0, TopicPartitionPosition.End()),
            (1, TopicPartitionPosition.End()),
            (2, TopicPartitionPosition.End())
          ),
          "shared.topic-to-consume" -> Iterable(
            (0, TopicPartitionPosition.Beginning())
          )
        )

        assert(actual == expected)
      }
    }

    "the application is not a kafka-streams application and the cluster has some topics" should {
      "not delete any topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeploySourceSink()

        val allTopicsOrError = IndexedSeq(
          "test.topic",
          "test.__kstreams__.Processor-something",
          "test.__kstreams__.Processor-something-repartition",
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.sink"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = IndexedSeq.empty.asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }

    "the application is a kafka-streams application but the cluster does not have any topics" should {
      "not delete any topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeployKafkaStreams()

        val allTopicsOrError = IndexedSeq.empty.asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.processor"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = IndexedSeq.empty.asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }

    "the application is a kafka-streams application and the cluster does have some topics but not internal" should {
      "not delete any topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeployKafkaStreams()

        val allTopicsOrError = IndexedSeq(
          "test.topic-input",
          "test.topic-intermediate",
          "test.topic-output"
        ).asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.processor"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = IndexedSeq.empty.asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }

    "the application is a kafka-streams application and the cluster does have some topics, internal ones too" should {
      "delete the internal topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeployKafkaStreams()

        val allTopicsOrError = IndexedSeq(
          "test.topic-input",
          "test.topic-intermediate",
          "test.topic-output",
          "test.__kstreams__.Processor-something",
          "test.__kstreams__.Processor-something-repartition",
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.processor"),
          ResetApplicationOptions.ApplicationTopics()
        )
        val expected = IndexedSeq(
          "test.__kstreams__.Processor-something-repartition",
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }
  }

  "ResetApplicationOptions.ApplicationTopics with no topics to delete" when {
    "the application is a kafka-streams application and the cluster does have some topics, internal ones too" should {
      "not delete any topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeployKafkaStreams()

        val allTopicsOrError = IndexedSeq(
          "test.topic-input",
          "test.topic-intermediate",
          "test.topic-output",
          "test.__kstreams__.Processor-something",
          "test.__kstreams__.Processor-something-repartition",
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.processor"),
          ResetApplicationOptions.ApplicationTopics(deleteKafkaStreamsInternalTopics = Iterable.empty)
        )
        val expected = IndexedSeq.empty.asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }
  }

  "ResetApplicationOptions.ApplicationTopics with only particular topics to delete" when {
    "the application is a kafka-streams application and the cluster does have some topics, internal ones too" should {
      "deletes only the chosen topics" in {
        val topologyId = TopologyEntityId("test")
        val topologyToDeploy = topologyToDeployKafkaStreams()

        val allTopicsOrError = IndexedSeq(
          "test.topic-input",
          "test.topic-intermediate",
          "test.topic-output",
          "test.__kstreams__.Processor-something",
          "test.__kstreams__.Processor-something-repartition",
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]
        val actual = calculateTopicsToBeDeleted(
          allTopicsOrError,
          Map(topologyId -> topologyToDeploy),
          topologyId,
          ApplicationId("test.processor"),
          ResetApplicationOptions.ApplicationTopics(deleteKafkaStreamsInternalTopics = Iterable(FlexibleName.Regex(""".*something-changelog""")))
        )
        val expected = IndexedSeq(
          "test.__kstreams__.Processor-something-changelog"
        ).asRight[Seq[CommandError]]

        assert(actual == expected)
      }
    }
  }
}
