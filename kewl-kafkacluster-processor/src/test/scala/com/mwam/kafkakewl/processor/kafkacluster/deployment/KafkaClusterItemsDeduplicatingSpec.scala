/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain.{TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.resource.PatternType
import org.scalatest.FlatSpec

class KafkaClusterItemsDeduplicatingSpec extends FlatSpec with TestTopologiesToDeploy with TestTopologiesToDeployCommon {
  "empty list of ACLs" should "be deduplicated to an empty list" in {
    assert(KafkaClusterItems.deduplicateAcls(Seq.empty) == Seq.empty)
  }

  "the same ACL twice" should "be deduplicated to a single ACL" in {
    def test(acl: KafkaClusterItem.Acl) = {
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, acl)) == Seq(acl))
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, acl.copy())) == Seq(acl))
    }

    test(KafkaClusterItems.allowTopic("topic", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("topic", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("group", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("group", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("transactionalId", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("transactionalId", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
  }

  "almost the same ACL twice" should "not be deduplicated to a single ACL" in {
    def test(acl: KafkaClusterItem.Acl) = {
      val almostTheSameAcl1 = acl.copy(operation = AclOperation.ALL)
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, almostTheSameAcl1)) == Seq(acl, almostTheSameAcl1))
      val almostTheSameAcl2 = acl.copy(host = "other")
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, almostTheSameAcl2)) == Seq(acl, almostTheSameAcl2))
      val almostTheSameAcl3 = acl.copy(principal = "service-other")
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, almostTheSameAcl3)) == Seq(acl, almostTheSameAcl3))
    }

    test(KafkaClusterItems.allowTopic("topic", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("topic", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("group", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("group", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("transactionalId", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("transactionalId", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
  }

  "A literal ACL and its prefix" should "be deduplicated to the prefix ACL" in {
    def test(acl: KafkaClusterItem.Acl) = {
      val prefixAcl =
        if (acl.resourceName == "*") acl.copy() // "*" permission can only be deduped, not prefix-deduped
        else acl.copy(resourceName = acl.resourceName.substring(0, 4), resourcePatternType = PatternType.PREFIXED)
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, prefixAcl)) == Seq(prefixAcl))

      val identicalPrefixAcl =
        if (acl.resourceName == "*") acl.copy() // "*" permission can only be deduped, not prefix-deduped
        else acl.copy(resourcePatternType = PatternType.PREFIXED)
      assert(KafkaClusterItems.deduplicateAcls(Seq(acl, identicalPrefixAcl)) == Seq(identicalPrefixAcl))
    }

    test(KafkaClusterItems.allowTopic("test.topic", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("test.topic", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("test.group", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("test.group", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("test.transactionalId", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("test.transactionalId", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
  }

  "prefix * acls" should "be ignored" in {
    def test(acl: KafkaClusterItem.Acl) = {
      if (acl.resourceName == "*" && acl.resourcePatternType == PatternType.PREFIXED) {
        val prefixAcl = acl.copy(resourcePatternType = PatternType.LITERAL)
        assert(KafkaClusterItems.deduplicateAcls(Seq(acl, prefixAcl)) == Seq(acl, prefixAcl))
      } else {
        val prefixAclToIgnore = acl.copy(resourceName = "*", resourcePatternType = PatternType.PREFIXED)
        assert(KafkaClusterItems.deduplicateAcls(Seq(acl, prefixAclToIgnore)) == Seq(acl, prefixAclToIgnore))
      }
    }

    test(KafkaClusterItems.allowTopic("test.topic", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("test.topic", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("test.group", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("test.group", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowGroup("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("test.transactionalId", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("test.transactionalId", "service-user", AclOperation.READ, PatternType.PREFIXED))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.LITERAL))
    test(KafkaClusterItems.allowTransactionalId("*", "service-user", AclOperation.READ, PatternType.PREFIXED))
  }

  "multiple literals and prefixes" should "be deduplicated correctly" in {
    val actualDeduplicatedAcls = KafkaClusterItems.deduplicateAcls(Seq(
      KafkaClusterItems.allowTopic("test.project.topic1", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project.topic2", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project", "service-user", AclOperation.READ, PatternType.PREFIXED),
      KafkaClusterItems.allowTopic("test", "service-user", AclOperation.READ, PatternType.LITERAL)
    ))

    val expectedDeduplicatedAcls = Seq(
      KafkaClusterItems.allowTopic("test.project", "service-user", AclOperation.READ, PatternType.PREFIXED),
      KafkaClusterItems.allowTopic("test", "service-user", AclOperation.READ, PatternType.LITERAL)
    )

    assert(actualDeduplicatedAcls == expectedDeduplicatedAcls)
  }

  "multiple literals and prefixes with a wildcard" should "be deduplicated to the wildcard only" in {
    val actualDeduplicatedAcls = KafkaClusterItems.deduplicateAcls(Seq(
      KafkaClusterItems.allowTopic("test.project.topic1", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project.topic2", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project", "service-user", AclOperation.READ, PatternType.PREFIXED),
      KafkaClusterItems.allowTopic("test", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL)
    ))

    val expectedDeduplicatedAcls = Seq(
      KafkaClusterItems.allowTopic("*", "service-user", AclOperation.READ, PatternType.LITERAL)
    )

    assert(actualDeduplicatedAcls == expectedDeduplicatedAcls)
  }

  "multiple literals and prefixes some with slight differences" should "be deduplicated correctly" in {
    val actualDeduplicatedAcls = KafkaClusterItems.deduplicateAcls(Seq(
      KafkaClusterItems.allowTopic("test.project.topic1", "service-user", AclOperation.WRITE, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project.topic2", "service-user", AclOperation.READ, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project", "service-user", AclOperation.READ, PatternType.PREFIXED),
      KafkaClusterItems.allowTopic("test", "service-user", AclOperation.DESCRIBE, PatternType.LITERAL)
    ))

    val expectedDeduplicatedAcls = Seq(
      KafkaClusterItems.allowTopic("test.project.topic1", "service-user", AclOperation.WRITE, PatternType.LITERAL),
      KafkaClusterItems.allowTopic("test.project", "service-user", AclOperation.READ, PatternType.PREFIXED),
      KafkaClusterItems.allowTopic("test", "service-user", AclOperation.DESCRIBE, PatternType.LITERAL)
    )

    assert(actualDeduplicatedAcls == expectedDeduplicatedAcls)
  }
}
