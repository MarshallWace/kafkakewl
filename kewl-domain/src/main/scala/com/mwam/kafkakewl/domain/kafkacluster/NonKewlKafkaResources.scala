/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafkacluster

import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}

/**
  * Describes a non-kewl acl. None is considered matching any value. The only regex is the resourceNameRegex,
  * the rest matches with exact equality (or None matching everything).
  *
  * I don't think it makes sense to use regex for any other property of the ACLs.
  */
final case class NonKewlKafkaAcl(
  resourceType: Option[String] = None,
  resourcePatternType: Option[String] = None,
  resourceNameRegex: Option[String] = None,
  principal: Option[String] = None,
  host: Option[String] = None,
  operation: Option[String] = None,
  permission: Option[String] = None
) {
  /**
    * Makes the resource name regex to be whole-string regex.
    */
  def makeResourceNameRegexWholeString: NonKewlKafkaAcl = copy(resourceNameRegex = resourceNameRegex.map(RegexBuilder.makeRegexWholeString))
}

/**
  * Contains regular expressions to identify kafka resources (topics, consumer group names, transactional-ids and ACLs)
  * that do not belong to kafka kewl and must not be touched or impacted in any way.
  *
  * @param topicRegexes the non-kewl topic regular expressions (are interpreted by default as whole-string) - any topic matching these is considered non-kewl.
  * @param topicForAclsRegexes the non-kewl topic regular expressions (are interpreted by default as whole-string) - any acl matching these is considered non-kewl.
  * @param groupForAclsRegexes the non-kewl group regular expressions (are interpreted by default as whole-string) - any acl matching these is considered non-kewl.
  * @param transactionalIdForAclsRegexes the non-kewl transactional id regular expressions (are interpreted by default as whole-string) - any acl matching these is considered non-kewl.
  * @param kafkaAcls the non-kewl acls, if something can't be covered by the previous ones.
  */
final case class NonKewlKafkaResources(
  topicRegexes: Seq[String] = Seq.empty,
  topicForAclsRegexes: Seq[String] = Seq.empty,
  groupForAclsRegexes: Seq[String] = Seq.empty,
  transactionalIdForAclsRegexes: Seq[String] = Seq.empty,
  kafkaAcls: Seq[NonKewlKafkaAcl] = Seq.empty
) {
  private[this] val topicRegexesWholeString = topicRegexes.map(RegexBuilder.makeRegexWholeString)
  private[this] val topicForAclsRegexesWholeString = topicForAclsRegexes.map(RegexBuilder.makeRegexWholeString)
  private[this] val groupForAclsRegexesWholeString = groupForAclsRegexes.map(RegexBuilder.makeRegexWholeString)
  private[this] val transactionalIdForAclsRegexesWholeString = transactionalIdForAclsRegexes.map(RegexBuilder.makeRegexWholeString)
  private[this] val kafkaAclsWholeString = kafkaAcls.map(_.makeResourceNameRegexWholeString)

  private def regexMatches(regex: String, str: String): Boolean = regex.r.findFirstIn(str).isDefined

  def isTopicNonKewl(topic: String): Boolean = topicRegexesWholeString.exists(regexMatches(_, topic))
  def isTopicNonKewlForAcls(topic: String): Boolean = topicForAclsRegexesWholeString.exists(regexMatches(_, topic))
  def isGrougNonKewl(group: String): Boolean = groupForAclsRegexesWholeString.exists(regexMatches(_, group))
  def isTransactionalIdNonKewl(group: String): Boolean = transactionalIdForAclsRegexesWholeString.exists(regexMatches(_, group))

  def isAclNonKewl(
    resourceType: ResourceType,
    resourcePatternType: PatternType,
    resourceName: String,
    principal: String,
    host: String,
    operation: AclOperation,
    permission: AclPermissionType
  ): Boolean = {
    val topicOrGroupOrTransactionalIdIsNonKewl = (resourceType, resourcePatternType, resourceName) match {
      // for any TOPIC acl (prefixed, literal or *) if there is any matching non-kewl topic regex, we considered this acl non-kewl.
      case (ResourceType.TOPIC, _, t) => isTopicNonKewlForAcls(t)
      // for any GROUP acl (prefixed, literal or *) if there is any matching non-kewl group regex, we considered this acl non-kewl.
      case (ResourceType.GROUP, _, g) => isGrougNonKewl(g)
      // for any TRANSACTIONAL_ID acl (prefixed, literal or *) if there is any matching non-kewl transactional id regex, we considered this acl non-kewl.
      case (ResourceType.TRANSACTIONAL_ID, _, t) => isTransactionalIdNonKewl(t)
      case _ => false
    }

    val aclNonKewl = kafkaAclsWholeString
      .exists(n => {
        // any None value matches every input
        n.resourceType.forall(_ == resourceType.toString) &&
        n.resourcePatternType.forall(_ == resourcePatternType.toString) &&
        // regex matching only here
        n.resourceNameRegex.forall(regexMatches(_, resourceName.toString)) &&
        n.principal.forall(_ == principal.toString) &&
        n.host.forall(_ == host.toString) &&
        n.operation.forall(_ == operation.toString) &&
        n.permission.forall(_ == permission.toString)
      })

    topicOrGroupOrTransactionalIdIsNonKewl || aclNonKewl
  }
}
