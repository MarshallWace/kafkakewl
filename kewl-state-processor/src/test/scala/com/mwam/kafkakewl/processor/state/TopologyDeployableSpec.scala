/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import cats.syntax.option._
import com.mwam.kafkakewl.domain
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{TestTopologies, TestTopologiesCommon, TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.{Matchers, WordSpec}

class TopologyDeployableSpec extends WordSpec
  with Matchers
  with MakeDeployableMatchers
  with TestTopologies
  with TestTopologiesCommon
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon
  with TopologyDeployableCommon {

  implicit class TopologyToDeployExtensionsMore(topologyToDeploy: TopologyToDeploy) {
    def updateApplication(applicationId: String, updateFunc: TopologyToDeploy.Application => TopologyToDeploy.Application): TopologyToDeploy = {
      topologyToDeploy.copy(
        applications = topologyToDeploy.applications.map {
          case (id, app) => (id, if (id.id == applicationId) updateFunc(app) else app)
        }
      )
    }

    def updateTopic(topicId: String, updateFunc: TopologyToDeploy.Topic => TopologyToDeploy.Topic): TopologyToDeploy = {
      topologyToDeploy.copy(
        topics = topologyToDeploy.topics.map {
          case (id, topic) => (id, if (id.id == topicId) updateFunc(topic) else topic)
        }
      )
    }

    def updateApplications(applicationIds: Set[String], updateFunc: TopologyToDeploy.Application => TopologyToDeploy.Application): TopologyToDeploy = {
      topologyToDeploy.copy(
        applications = topologyToDeploy.applications.map {
          case (id, app) => (id, if (applicationIds(id.id)) updateFunc(app) else app)
        }
      )
    }
  }

  "a topology with no environment variables and no expressions" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }

    "deployed to prod-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
        val actualTopologyToDeploy = prodClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.TopicReadOnly, deployWithAuthorizationCode = true.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with some environment variables and no expressions" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink().copy(
          environments =
            env("default", vars("a", "1")) ++
              env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
              env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
        )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }

    "deployed to prod-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink().copy(
          environments =
            env("default", vars("a", "1")) ++
              env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
              env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
        )
        val actualTopologyToDeploy = prodClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.TopicReadOnly, deployWithAuthorizationCode = true.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with a environment variable for the user-name" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("service-user", "service-some-service-user-test")) ++ env("prod-cluster", vars("service-user", "service-some-service-user-prod")))
          .updateApplications(Set("test.source", "test.sink"), a => a.copy(user = "${service-user}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateApplications(Set("test.source", "test.sink"), a => a.copy(user = "service-some-service-user-test"))
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }

    "deployed to prod-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("service-user", "service-some-service-user-test")) ++ env("prod-cluster", vars("service-user", "service-some-service-user-prod")))
          .updateApplications(Set("source", "sink"), a => a.copy(user = "${service-user}"))
        val actualTopologyToDeploy = prodClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.TopicReadOnly, deployWithAuthorizationCode = true.some)
          .updateApplications(Set("source", "sink"), a => a.copy(user = "service-some-service-user-prod"))
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with a missing environment variable for the user-name" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("service-user", "service-some-service-user-test")) ++ env("prod-cluster", vars("service-user", "service-some-service-user-prod")))
          .updateApplications(Set("source", "sink"), a => a.copy(user = "${service-userX}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateApplications(Set("source", "sink"), a => a.copy(user = "service-some-service-user-test"))
        actualTopologyToDeploy should containsError("in application 'test.source': variable 'service-userX' is not defined in 'user'")
        actualTopologyToDeploy should containsError("in application 'test.sink': variable 'service-userX' is not defined in 'user'")
      }
    }
  }

  "a topology with a environment variable for the number of partitions" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("partitions", "9")) ++ env("prod-cluster", vars("partitions", "13")))
          .updateTopic("topic", t => t.copy(partitions = "${partitions}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(partitions = 9))
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with a missing environment variable for the number of partitions" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("partitions", "9")) ++ env("prod-cluster", vars("partitions", "13")))
          .updateTopic("topic", t => t.copy(partitions = "${partitionsX}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(partitions = 1))
        actualTopologyToDeploy should containsError("in topic 'test.topic': variable 'partitionsX' is not defined in 'partitions'")
      }
    }
  }

  "a topology with an empty environment variable for the number of partitions" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy with the default number of partitions" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("partitions")) ++ env("prod-cluster", vars("partitions")))
          .updateTopic("topic", t => t.copy(partitions = "${partitions}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(partitions = 1))
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with a non-integer environment variable for the number of partitions" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("partitions", "a")) ++ env("prod-cluster", vars("partitions", "b")))
          .updateTopic("topic", t => t.copy(partitions = "${partitions}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(partitions = 1))
        actualTopologyToDeploy should containsError("expecting 'partitions' to be an integer")
      }
    }
  }

  "a topology with a multi-value environment variable for the number of partitions" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("partitions", "1", "2")) ++ env("prod-cluster", vars("partitions", "3", "4")))
          .updateTopic("topic", t => t.copy(partitions = "${partitions}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(partitions = 1))
        actualTopologyToDeploy should containsError("expecting 'partitions' to be a scalar")
      }
    }
  }

  "a topology with a environment variable for the retention.ms topic config" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("retention.ms", "333")) ++ env("prod-cluster", vars("retention.ms", "999")))
          .updateTopic("topic", t => t.copy(config = Map("retention.ms" -> "${retention.ms}")))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(config = Map("retention.ms" -> "333")))
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with a missing environment variable for the retention.ms topic config" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink()
          .copy(environments = env("test-cluster", vars("retention.ms", "333")) ++ env("prod-cluster", vars("retention.ms", "999")))
          .updateTopic("topic", t => t.copy(config = Map("retention.ms" -> "${retention.msX}")))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .updateTopic("topic", t => t.copy(config = Map.empty))
        actualTopologyToDeploy should containsError("in topic 'test.topic': variable 'retention.msX' is not defined in 'topic config's value'")
      }
    }
  }

  "a topology with a environment variable tag" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(
            environments = env("test-cluster", vars("some-key", "xyz")) ++ env("prod-cluster", vars("some-key", "abc")),
            tags = Seq("tag1-${some-key}", "")
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .copy(
            tags = Seq("tag1-xyz")
          )
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with environment variable labels" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(
            environments = env("test-cluster", vars("some-key", "xyz")) ++ env("prod-cluster", vars("some-key", "abc")),
            labels = Map[String, String]("key1-${some-key}" -> "constant", "" -> "3", "key3" -> "${some-key}")
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
          .copy(
            labels = Map("key1-xyz" -> "constant", "key3" -> "xyz")
          )
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with environment variable = full developers-access" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(
            environments = env("test-cluster", vars("dev-access", "full")),
            developersAccess = Topology.DevelopersAccessExpr("${dev-access}")
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.Full, deployWithAuthorizationCode = false.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with environment variable = topicreadonly developers-access" when {
    "deployed to test-cluster" should {
      "get converted to a topology-to-deploy" in {
        val topology = topologySourceSink()
          .copy(
            environments = env("test-cluster", vars("dev-access", "topicreadonly")),
            developersAccess = Topology.DevelopersAccessExpr("${dev-access}")
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy = topologyToDeploySourceSink(developersAccess = DevelopersAccess.TopicReadOnly, deployWithAuthorizationCode = false.some)
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with missing environment variable developers-access" when {
    "deployed to test-cluster" should {
      "fail" in {
        val topology = topologySourceSink().copy(developersAccess = Topology.DevelopersAccessExpr("${dev-access}"))
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        actualTopologyToDeploy should containsError("variable 'dev-access' is not defined in 'developersAccess'")
      }
    }
  }

  "a topology with regex topic relationships" when {
    "deployed to test-cluster" should {
      "generate the correct kafka cluster items" in {
        val topology =
          Topology(
            topology = TopologyId("test"),
            topics = Map(
              "topic1" -> Topology.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
              "topic2" -> Topology.Topic("test.topic2", config = Map("retention.ms" -> "31536000000")),
              "topic3" -> Topology.Topic("test.topic3", config = Map("retention.ms" -> "31536000000")),
              "topic4" -> Topology.Topic("test.topic4", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "consumer" -> Topology.Application("service-consumer").makeSimple(Some("test.consumer"))
            ).toMapByApplicationId,
            aliases = LocalAliases(topics = Map(LocalAliasId("topics") -> Seq(FlexibleNodeId.Regex("""test\.topic\d""")))),
            relationships = Map(
              relationshipFrom("consumer", (RelationshipType.Consume(), Seq(("topics", None)))),
            )
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy =
          domain.topology.TopologyToDeploy(
            topology = TopologyId("test"),
            developersAccess = DevelopersAccess.Full,
            deployWithAuthorizationCode = false.some,
            topics = Map(
              "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
              "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000")),
              "topic3" -> TopologyToDeploy.Topic("test.topic3", config = Map("retention.ms" -> "31536000000")),
              "topic4" -> TopologyToDeploy.Topic("test.topic4", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "consumer" -> TopologyToDeploy.Application("service-consumer").makeSimple(Some("test.consumer"))
            ).toMapByApplicationId,
            aliases = LocalAliases(topics = Map(LocalAliasId("topics") -> Seq(FlexibleNodeId.Regex("""test\.topic\d""")))),
            relationships = Map(
              toDeployRelationshipFrom("consumer", (RelationshipType.Consume(), Seq(("topics", None))))
            )
          )
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology with regex topic and application relationships" when {
    "deployed to test-cluster" should {
      "generate the correct kafka cluster items" in {
        val topology: Topology =
          Topology(
            topology = TopologyId("test"),
            topics = Map(
              "topic1" -> Topology.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
              "topic2" -> Topology.Topic("test.topic2", config = Map("retention.ms" -> "31536000000")),
              "topic3" -> Topology.Topic("test.topic3", config = Map("retention.ms" -> "31536000000")),
              "topic4" -> Topology.Topic("test.topic4", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "consumer1" -> Topology.Application("service-consumer").makeSimple(Some("test.consumer1")),
              "consumer2" -> Topology.Application("service-consumer").makeSimple(Some("test.consumer2"))
            ).toMapByApplicationId,
            aliases = LocalAliases(
              topics = Map(LocalAliasId("topics") ->  Seq(FlexibleNodeId.Prefix("test.topic"))),
              applications = Map(LocalAliasId("TestApps") -> Seq(FlexibleNodeId.Namespace("test")))
            ),
            relationships = Map(
              relationshipFrom("TestApps", (RelationshipType.Consume(), Seq(("topics", None)))),
            )
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy =
          domain.topology.TopologyToDeploy(
            topology = TopologyId("test"),
            developersAccess = DevelopersAccess.Full,
            deployWithAuthorizationCode = false.some,
            topics = Map(
              "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
              "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000")),
              "topic3" -> TopologyToDeploy.Topic("test.topic3", config = Map("retention.ms" -> "31536000000")),
              "topic4" -> TopologyToDeploy.Topic("test.topic4", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "consumer1" -> TopologyToDeploy.Application("service-consumer").makeSimple(Some("test.consumer1")),
              "consumer2" -> TopologyToDeploy.Application("service-consumer").makeSimple(Some("test.consumer2"))
            ).toMapByApplicationId,
            aliases = LocalAliases(
              topics = Map(LocalAliasId("topics") ->  Seq(FlexibleNodeId.Prefix("test.topic"))),
              applications = Map(LocalAliasId("TestApps") -> Seq(FlexibleNodeId.Namespace("test")))
            ),
            relationships = Map(
              toDeployRelationshipFrom("TestApps", (RelationshipType.Consume(), Seq(("topics", None)))),
            )
          )
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }

  "a topology referencing another with regex topic relationships" when {
    "deployed to test-cluster" should {
      "generate the correct kafka cluster items" in {
        val topology: Topology =
          Topology(
            topology = TopologyId("test"),
            topics = Map(
              "shared.derived.topic" -> Topology.Topic("test.shared.derived.topic", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "processor" -> Topology.Application("service-test-processor").makeSimple(Some("test.processor"))
            ).toMapByApplicationId,
            aliases = LocalAliases(topics = Map(LocalAliasId("shared.topics") -> Seq(FlexibleNodeId.Regex("""test\.shared\.topic\d""")))),
            relationships = Map(
              relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared.topics", None)))),
              relationshipFrom("processor", (RelationshipType.Produce(), Seq(("shared.derived.topic", None))))
            )
          )
        val actualTopologyToDeploy = testClusterMakeDeployable(topology)
        val expectedTopologyToDeploy =
          domain.topology.TopologyToDeploy(
            topology = TopologyId("test"),
            developersAccess = DevelopersAccess.Full,
            deployWithAuthorizationCode = false.some,
            topics = Map(
              "shared.derived.topic" -> TopologyToDeploy.Topic("test.shared.derived.topic", config = Map("retention.ms" -> "31536000000"))
            ).toMapByTopicId,
            applications = Map(
              "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
            ).toMapByApplicationId,
            aliases = LocalAliases(topics = Map(LocalAliasId("shared.topics") -> Seq(FlexibleNodeId.Regex("""test\.shared\.topic\d""")))),
            relationships = Map(
              toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared.topics", None)))),
              toDeployRelationshipFrom("processor", (RelationshipType.Produce(), Seq(("shared.derived.topic", None))))
            )
          )
        actualTopologyToDeploy should makeDeployable(expectedTopologyToDeploy)
      }
    }
  }
}
