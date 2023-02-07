/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{DeploymentAllowUnsafeKafkaClusterChange, UnsafeKafkaClusterChangeDescription, UnsafeKafkaClusterChangeOperation, UnsafeKafkaClusterChangePropertyDescription}
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys
import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}
import org.scalatest.{Matchers, WordSpec}

final case class KafkaClusterApprovedChangesPrettyWrapper(inner: Map[String, KafkaClusterApprovedChange]) {
  override def toString: String = inner.toList.sortBy(_._1).map(_._2).mkString("(\n  ", "\n  ", "\n)")
}

class DeployTopologyUnsafeChangesSpec extends WordSpec with Matchers {
  implicit class KafkaClusterApprovedChangesExtensions(inner: Map[String, KafkaClusterApprovedChange]) {
    def prettify = KafkaClusterApprovedChangesPrettyWrapper(inner)
  }

  implicit class KafkaClusterItemTopicExtensions(topic: KafkaClusterItem.Topic) {
    def ++(additionalConfig: Map[String, String]): KafkaClusterItem.Topic = topic.copy(config = topic.config ++ additionalConfig)
    def +(additionalConfig: (String, String)): KafkaClusterItem.Topic = topic.copy(config = topic.config + additionalConfig)
    def --(withoutConfig: Seq[String]): KafkaClusterItem.Topic = topic.copy(config = topic.config -- withoutConfig)
    def -(withoutConfig: String): KafkaClusterItem.Topic = topic.copy(config = topic.config - withoutConfig)
  }

  private def noUnsafeChangeIsAllowed(unsafeKafkaClusterChangeDescription: UnsafeKafkaClusterChangeDescription): Boolean = false
  private def isUnsafeChangeAllowed(
    allowedUnsafeChanges: Seq[DeploymentAllowUnsafeKafkaClusterChange]
  )(unsafeKafkaClusterChangeDescription: UnsafeKafkaClusterChangeDescription): Boolean =
    unsafeKafkaClusterChangeDescription.isAllowed(allowedUnsafeChanges)

  private val testTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
  private val anotherTestTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map("retention.ms" -> "31536000000"))
  private val testReadTopicAcl = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW)
  private val testReadAnotherTopicAcl = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.another-topic", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW)

  private def fromKafkaClusterChanges(
    isUnsafeChangeAllowed: UnsafeKafkaClusterChangeDescription => Boolean
  )(plannedChanges: Seq[KafkaClusterChange]): Map[String, KafkaClusterApprovedChange] =
    KafkaClusterApprovedChange.fromKafkaClusterChanges(isUnsafeChangeAllowed)(plannedChanges).map(_.toTuple).toMap

  private def makeUnsafeNotAllowed(removeChange: KafkaClusterChange.Remove): (String, KafkaClusterApprovedChange) =
    KafkaClusterApprovedChange(
      removeChange,
      KafkaClusterCommandActionSafety.UnsafeNotAllowed,
      Some(UnsafeKafkaClusterChangeDescription(UnsafeKafkaClusterChangeOperation.Remove, removeChange.item.entityType, removeChange.key)),
      None
    ).toTuple

  private def makeUnsafePartiallyAllowed(
    updateTopicChange: KafkaClusterChange.UpdateTopic,
    allowedUpdateTopicChange: KafkaClusterChange.UpdateTopic,
    unsafeEntityProperties: Seq[UnsafeKafkaClusterChangePropertyDescription]
  ): (String, KafkaClusterApprovedChange) =
    KafkaClusterApprovedChange(
      updateTopicChange,
      KafkaClusterCommandActionSafety.UnsafePartiallyAllowed,
      Some(UnsafeKafkaClusterChangeDescription(UnsafeKafkaClusterChangeOperation.Update, KafkaClusterItemEntityType.Topic, updateTopicChange.key, unsafeEntityProperties)),
      Some(allowedUpdateTopicChange)
    ).toTuple

  private def makeUnsafePartiallyAllowedOfConfigChange(configKey: String)
    (updateTopicChange: KafkaClusterChange.UpdateTopic): (String, KafkaClusterApprovedChange) =
    KafkaClusterApprovedChange(
      updateTopicChange,
      KafkaClusterCommandActionSafety.UnsafePartiallyAllowed,
      Some(UnsafeKafkaClusterChangeDescription(
        UnsafeKafkaClusterChangeOperation.Update,
        KafkaClusterItemEntityType.Topic,
        updateTopicChange.key,
        Seq(unsafeKafkaClusterChangePropertyDescription(updateTopicChange, configKey))
      )),
      Some(topicChangeWithoutConfigChange(updateTopicChange, configKey))
    ).toTuple

  private def unsafeKafkaClusterChangePropertyDescription(updateTopicChange: KafkaClusterChange.UpdateTopic, configKey: String): UnsafeKafkaClusterChangePropertyDescription = {
    UnsafeKafkaClusterChangePropertyDescription(
      configKey,
      s"${updateTopicChange.beforeItem.config.getOrElse(configKey, "")}->${updateTopicChange.afterItem.config.getOrElse(configKey, "")}"
    )
  }

  private def topicChangeWithoutConfigChange(updateTopicChange: KafkaClusterChange.UpdateTopic, configKey: String): KafkaClusterChange.UpdateTopic =
    KafkaClusterChange.UpdateTopic(
      updateTopicChange.beforeItem,
      if (updateTopicChange.beforeItem.config.contains(configKey))
        updateTopicChange.afterItem + (configKey -> updateTopicChange.beforeItem.config(configKey))
      else
        updateTopicChange.afterItem - configKey
    )

  private def makeUnsafeAllowed(change: KafkaClusterChange): (String, KafkaClusterApprovedChange) =
    KafkaClusterApprovedChange(
      change,
      KafkaClusterCommandActionSafety.UnsafeAllowed,
      None,
      Some(change)
    ).toTuple

  private def makeSafe(change: KafkaClusterChange): (String, KafkaClusterApprovedChange) =
    KafkaClusterApprovedChange(
      change,
      KafkaClusterCommandActionSafety.Safe,
      None,
      Some(change)
    ).toTuple

  "Creating topics and ACLs" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val addTopic = KafkaClusterChange.Add(testTopic)
        val addAcl = KafkaClusterChange.Add(testReadTopicAcl)
        val changes = Seq(addTopic, addAcl)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Removing topics and ACLs" when {
    "no unsafe change is allowed" should {
      "be unsafe" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeNotAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all UPDATE change is allowed" should {
      "be unsafe" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Update)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeNotAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "another topic's changes are allowed" should {
      "be unsafe" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(anotherTestTopic.key)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeNotAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all REMOVE changes are allowed" should {
      "be unsafe but allowed" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Remove)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "REMOVE topic changes are allowed" should {
      "be unsafe but allowed for the topic removal but unsafe and not allowed for the ACL" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Remove), entityType = Some(KafkaClusterItemEntityType.Topic)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = Map(makeUnsafeAllowed(removeTopic), makeUnsafeNotAllowed(removeAcl))

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "any topic changes are allowed" should {
      "be unsafe but allowed for the topic removal but unsafe and not allowed for the ACL" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityType = Some(KafkaClusterItemEntityType.Topic)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = Map(makeUnsafeAllowed(removeTopic), makeUnsafeNotAllowed(removeAcl))

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "this topic's changes are allowed" should {
      "be unsafe but allowed for the topic removal but unsafe and not allowed for the ACL" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = Map(makeUnsafeAllowed(removeTopic), makeUnsafeNotAllowed(removeAcl))

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "REMOVING topics and ACLs are allowed" should {
      "be unsafe but allowed" in {
        val removeTopic = KafkaClusterChange.Remove(testTopic)
        val removeAcl = KafkaClusterChange.Remove(testReadTopicAcl)
        val changes = Seq(removeTopic, removeAcl)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Remove),
            entityType = Some(KafkaClusterItemEntityType.Topic)
          ),
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Remove),
            entityType = Some(KafkaClusterItemEntityType.Acl)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Update a topic config other than retention.ms and cleanup.policy" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic, testTopic + ("config1" -> "b"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("config1" -> "a"), testTopic)
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic + ("config1" -> "a"), testTopic + ("config1" -> "b"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Increase a topic's retention.ms" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31536000001"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "41536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604800000"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604800001"))
        val updateTopic5 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "1604800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4, updateTopic5)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Decrease a topic's retention.ms" when {
    "no unsafe change is allowed" should {
      "be unsafe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafePartiallyAllowedOfConfigChange("retention.ms")).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all UPDATE change is allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Update)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all topic change is allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityType = Some(KafkaClusterItemEntityType.Topic)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "the topic's changes are allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "the topic's retention.ms changes are allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key),
            entityPropertyName = Some("retention.ms")
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "the topic's cleanup.policy changes are allowed" should {
      "be unsafe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "31535999999"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("retention.ms" -> "31536000000"), testTopic + ("retention.ms" -> "21536000000"))
        val updateTopic3 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "604799999"))
        val updateTopic4 = KafkaClusterChange.UpdateTopic(testTopic - "retention.ms", testTopic + ("retention.ms" -> "504800000"))
        val changes = Seq(updateTopic1, updateTopic2, updateTopic3, updateTopic4)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key),
            entityPropertyName = Some("cleanup.policy")
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafePartiallyAllowedOfConfigChange("retention.ms")).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's cleanup.policy so that it compacts" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "delete"), testTopic + ("cleanup.policy" -> "compact"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic - "cleanup.policy", testTopic + ("cleanup.policy" -> "compact"))
        val changes = Seq(updateTopic1, updateTopic2)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's cleanup.policy so that it does not compact" when {
    "no unsafe change is allowed" should {
      "be unsafe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic + ("cleanup.policy" -> "delete"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic - "cleanup.policy")
        val changes = Seq(updateTopic1, updateTopic2)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafePartiallyAllowedOfConfigChange("cleanup.policy")).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all UPDATE change is allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic + ("cleanup.policy" -> "delete"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic - "cleanup.policy")
        val changes = Seq(updateTopic1, updateTopic2)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            operation = Some(UnsafeKafkaClusterChangeOperation.Update)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "all topic change is allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic + ("cleanup.policy" -> "delete"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic - "cleanup.policy")
        val changes = Seq(updateTopic1, updateTopic2)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityType = Some(KafkaClusterItemEntityType.Topic)
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "the topic's cleanup.policy changes are allowed" should {
      "be unsafe but allowed" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic + ("cleanup.policy" -> "delete"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic - "cleanup.policy")
        val changes = Seq(updateTopic1, updateTopic2)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key),
            entityPropertyName = Some("cleanup.policy")
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafeAllowed).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }

    "the topic's retention.ms changes are allowed" should {
      "be unsafe" in {
        val updateTopic1 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic + ("cleanup.policy" -> "delete"))
        val updateTopic2 = KafkaClusterChange.UpdateTopic(testTopic + ("cleanup.policy" -> "compact"), testTopic - "cleanup.policy")
        val changes = Seq(updateTopic1, updateTopic2)

        val allowUnsafeChanges = Seq(
          DeploymentAllowUnsafeKafkaClusterChange(
            entityKey = Some(testTopic.key),
            entityPropertyName = Some("retention.ms")
          )
        )
        val actualChangesWithApproval = fromKafkaClusterChanges(isUnsafeChangeAllowed(allowUnsafeChanges))(changes)
        val expectedChangesWithApproval = changes.map(makeUnsafePartiallyAllowedOfConfigChange("cleanup.policy")).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's replicationFactor to be positive from another positive value" when {
    "no unsafe change is allowed" should {
      "be unsafe" in {
        val beforeTopic = testTopic.withReplicationFactor(3)
        val afterTopic = testTopic.withReplicationFactor(6)
        val updateTopic = KafkaClusterChange.UpdateTopic(beforeTopic, afterTopic)
        val changes = Seq(updateTopic)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = Seq(
          makeUnsafePartiallyAllowed(
            updateTopic,
            updateTopic.copy(afterItem = afterTopic.withReplicationFactor(3)),
            Seq(UnsafeKafkaClusterChangePropertyDescription("replicationFactor", "3->6")))
        ).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's replicationFactor to be positive from -1" when {
    "no unsafe change is allowed" should {
      "be unsafe" in {
        val beforeTopic = testTopic.withReplicationFactor(-1).withConfig(TopicConfigKeys.confluentPlacementConstraints, "{}")
        val afterTopic = testTopic.withReplicationFactor(3)
        val updateTopic = KafkaClusterChange.UpdateTopic(beforeTopic, afterTopic)
        val changes = Seq(updateTopic)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = Seq(
          makeUnsafePartiallyAllowed(
            updateTopic,
            updateTopic.copy(afterItem = afterTopic.withReplicationFactor(-1).withConfig(TopicConfigKeys.confluentPlacementConstraints, "{}")),
            Seq(UnsafeKafkaClusterChangePropertyDescription("replicationFactor", "-1->3")))
        ).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's replicationFactor to be positive from the same positive value" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val beforeTopic = testTopic.withReplicationFactor(3)
        val afterTopic = testTopic.withReplicationFactor(3)
        val updateTopic = KafkaClusterChange.UpdateTopic(beforeTopic, afterTopic)
        val changes = Seq(updateTopic)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's replicationFactor to be -1 from -1" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val beforeTopic = testTopic.withReplicationFactor(-1).withConfig(TopicConfigKeys.confluentPlacementConstraints, "{}")
        val afterTopic = testTopic.withReplicationFactor(-1).withConfig(TopicConfigKeys.confluentPlacementConstraints, "{}")
        val updateTopic = KafkaClusterChange.UpdateTopic(beforeTopic, afterTopic)
        val changes = Seq(updateTopic)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }

  "Changing a topic's replicationFactor to be -1 from a positive value" when {
    "no unsafe change is allowed" should {
      "be safe" in {
        val beforeTopic = testTopic.withReplicationFactor(3)
        val afterTopic = testTopic.withReplicationFactor(-1).withConfig(TopicConfigKeys.confluentPlacementConstraints, "{}")
        val updateTopic = KafkaClusterChange.UpdateTopic(beforeTopic, afterTopic)
        val changes = Seq(updateTopic)

        val actualChangesWithApproval = fromKafkaClusterChanges(noUnsafeChangeIsAllowed)(changes)
        val expectedChangesWithApproval = changes.map(makeSafe).toMap

        actualChangesWithApproval.prettify shouldBe expectedChangesWithApproval.prettify
      }
    }
  }
}
