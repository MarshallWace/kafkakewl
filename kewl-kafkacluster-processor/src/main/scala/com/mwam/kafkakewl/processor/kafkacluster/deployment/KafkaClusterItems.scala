/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import cats.syntax.option._
import com.mwam.kafkakewl.common.topology.TopologyLikeOperations
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys
import com.mwam.kafkakewl.domain.kafkacluster.ResolveTopicConfig
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}

private[kafkacluster] object KafkaClusterItems extends TopologyLikeOperations[TopologyToDeploy.Node, TopologyToDeploy.Topic, TopologyToDeploy.Application, TopologyToDeploy.RelationshipProperties] {
  def aclUserPrincipal(user: String): String = s"User:$user"
  def aclHost(host: Option[String]): String = host.getOrElse("*")

  def allow(
    resourceType: ResourceType,
    resourceName: String,
    user: String,
    operation: AclOperation,
    patternType: PatternType = PatternType.LITERAL,
    hostOrNone: Option[String] = None
  ): KafkaClusterItem.Acl = {
    if (resourceName.isEmpty && patternType == PatternType.PREFIXED) {
      // if we want prefix acl for empty resource name (i.e. for everything), kafka doesn't like it, it prefers LITERAL "*" permission
      KafkaClusterItem.Acl(resourceType, PatternType.LITERAL, "*", aclUserPrincipal(user), aclHost(hostOrNone), operation, AclPermissionType.ALLOW)
    } else {
      KafkaClusterItem.Acl(resourceType, patternType, resourceName, aclUserPrincipal(user), aclHost(hostOrNone), operation, AclPermissionType.ALLOW)
    }
  }

  def allowCluster(
    user: String,
    operation: AclOperation,
    hostOrNone: Option[String] = None
  ): KafkaClusterItem.Acl = allow(ResourceType.CLUSTER, "kafka-cluster", user, operation, PatternType.LITERAL, hostOrNone)

  def allowTopic(
    resourceName: String,
    user: String,
    operation: AclOperation,
    patternType: PatternType = PatternType.LITERAL,
    hostOrNone: Option[String] = None
  ): KafkaClusterItem.Acl = allow(ResourceType.TOPIC, resourceName, user, operation, patternType, hostOrNone)

  def allowGroup(
    resourceName: String,
    user: String,
    operation: AclOperation,
    patternType: PatternType = PatternType.LITERAL,
    hostOrNone: Option[String] = None
  ): KafkaClusterItem.Acl = allow(ResourceType.GROUP, resourceName, user, operation, patternType, hostOrNone)

  def allowTransactionalId(
    resourceName: String,
    user: String,
    operation: AclOperation,
    patternType: PatternType = PatternType.LITERAL,
    hostOrNone: Option[String] = None
  ): KafkaClusterItem.Acl = allow(ResourceType.TRANSACTIONAL_ID, resourceName, user, operation, patternType, hostOrNone)

  /**
    * Removes the acls which are covered by any PREFIXED acl
    *
    * @param acls the acls to deduplicate
    * @return the deduplicated acls
    */
  def deduplicateAcls(acls: Seq[KafkaClusterItem.Acl]): Seq[KafkaClusterItem.Acl] = {
    case class PrefixedAclKey(
      resourceType: ResourceType,
      principal: String,
      host: String,
      operation: AclOperation,
      permission: AclPermissionType
    )
    def prefixedAclKey(acl: KafkaClusterItem.Acl) = PrefixedAclKey(acl.resourceType, acl.principal, acl.host, acl.operation, acl.permission)

    val distinctAcls = acls.distinct

    // prefixed ACLs keyed by everything but the actual prefix name
    val prefixedAcls = distinctAcls
      .collect {
        case acl if acl.resourcePatternType == PatternType.PREFIXED && acl.resourceName != "*" => (prefixedAclKey(acl), (acl.resourceName, acl))
        case acl if acl.resourcePatternType == PatternType.LITERAL && acl.resourceName == "*" => (prefixedAclKey(acl), ("", acl))
        // I don't know what PREFIXED "*" exactly does, so let's ignore that
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2))

    distinctAcls
      .filter { acl =>
      // if an acl has a prefix one which is exactly the same but a prefix (and they are not the same one) -> we just remove it
      !prefixedAcls
        .get(prefixedAclKey(acl))
        .exists(
          _.exists {
            case(prefix, prefixedAcl) =>
              if (acl.resourcePatternType == PatternType.LITERAL && acl.resourceName == "*")
                // if the acl is a LITERAL "*", the only prefix we accept that covers it is ""
                (acl ne prefixedAcl) && prefix == ""
              else if (acl.resourceName != "*")
                (acl ne prefixedAcl) && acl.resourceName.startsWith(prefix)
              else
                // PREFIXED "*" are kept, no matter what (I don't know what they mean)
                false
          }
        )
    }
  }

  def forDevelopers(
    topologyId: TopologyEntityId,
    topologyToDeploy: TopologyToDeploy,
    resolvedRelationships: Iterable[TopologyToDeploy.ApplicationTopicRelationship],
    topicDefaults: TopicDefaults
  ): Seq[KafkaClusterItem.Acl] = {
    val namespace = topologyToDeploy.namespace
    val namespacePrefix = namespace.ns // just to be consistent, no trailing dot
    val developers = topologyToDeploy.developers
    val developersAccess = topologyToDeploy.developersAccess

    def personalConsumerGroupAcl(user: String) = Seq(
      // acl to read the user's personal consumer group (prefix)
      allowGroup(resourceName =  s"user.$user", user, AclOperation.READ, PatternType.PREFIXED)
    )

    def topologyNamespaceAcls(user: String): Seq[KafkaClusterItem.Acl] =
      if (namespace.isEmpty) {
        // if the namespace is empty, the developers don't get any namespace prefix permissions to avoid * acls
        // as a compensation they will be permissioned the same way as applications' users

        // they still get the AclOperation.IDEMPOTENT_WRITE ACL in case of DevelopersAccess.Full
        developersAccess match {
          case DevelopersAccess.Full => Seq(
            allowCluster(user, AclOperation.IDEMPOTENT_WRITE),
          )
          case DevelopersAccess.TopicReadOnly => Seq.empty
        }
      } else {
        developersAccess match {
          case DevelopersAccess.Full => Seq(
            // in case of full-access: full prefixed access for this namespace (read/write/describe topics, read groups/all transactionalids, create topics[for kstreams])
            allowTopic(resourceName = namespacePrefix, user, AclOperation.DESCRIBE, PatternType.PREFIXED),
            allowTopic(resourceName = namespacePrefix, user, AclOperation.READ, PatternType.PREFIXED),
            allowTopic(resourceName = namespacePrefix, user, AclOperation.WRITE, PatternType.PREFIXED),
            allowGroup(resourceName = namespacePrefix, user, AclOperation.READ, PatternType.PREFIXED),
            allowCluster(user, AclOperation.IDEMPOTENT_WRITE),
            allowTransactionalId(resourceName = namespacePrefix, user, AclOperation.ALL, PatternType.PREFIXED),
            allowTopic(resourceName = namespacePrefix, user, AclOperation.CREATE, PatternType.PREFIXED),
            allowTopic(resourceName = namespacePrefix, user, AclOperation.DELETE, PatternType.PREFIXED)
          )

          case DevelopersAccess.TopicReadOnly => Seq(
            // topic read-only access is simpler, only read/describe topics (makes sense for prod deployments)
            allowTopic(resourceName = namespacePrefix, user, AclOperation.DESCRIBE, PatternType.PREFIXED),
            allowTopic(resourceName = namespacePrefix, user, AclOperation.READ, PatternType.PREFIXED)
          )
        }
      }

    def externalTopicAcls(user: String): Seq[KafkaClusterItem.Acl] =
      resolvedRelationships
      .filter(_.resolveNode2.topologyNode.topologyId != topologyId)
      .map(_.resolveNode2.topologyNode.node)
      .toSeq.distinct
      .flatMap(topic => {
        // getting acls topic-by-topic, no prefix here, because that could be too much
        val canBeConsumed = topic.canBeConsumedByNamespace(namespace, topicDefaults)
        val canBeProduced = topic.canBeProducedByNamespace(namespace, topicDefaults)
        val canBeConsumedOrProduced = canBeConsumed || canBeProduced
        val topicName = topic.name

        def someIf[T](b: Boolean, o: T): Option[T] = if (b) Some(o) else None

        val commonAcls = Seq(
          // in every case, for exposed topics, it gets describe
          someIf(canBeConsumedOrProduced, allowTopic(resourceName = topicName, user, AclOperation.DESCRIBE, PatternType.LITERAL)),
          // for consumable ones, it gets the read too
          someIf(canBeConsumed, allowTopic(resourceName = topicName, user, AclOperation.READ, PatternType.LITERAL))
        ).flatten

        val specificAcls = developersAccess match {
          case DevelopersAccess.Full => Seq(
            // with full-access only it gets the write for producable topics
            someIf(canBeProduced, allowTopic(resourceName = topicName, user, AclOperation.WRITE, PatternType.LITERAL))
          ).flatten
          case DevelopersAccess.TopicReadOnly => Seq.empty
        }

        commonAcls ++ specificAcls
    })

    for {
      user <- developers
      // deduplicating only the namespace and external topics acls (just to be more explicit and always have the personal consumer group ACL)
      acl <- personalConsumerGroupAcl(user) ++ deduplicateAcls(topologyNamespaceAcls(user) ++ externalTopicAcls(user))
    } yield acl
  }

  def forTopic(resolveTopicConfig: ResolveTopicConfig)(topic: TopologyToDeploy.Topic): Option[KafkaClusterItem.Topic] = {
    val actualTopicConfig = resolveTopicConfig(topic.replicaPlacement, topic.config)
    // if we have replica-placements just use -1 (the previous validation made sure that topic.replicationFactor must be empty),
    // otherwise use topic.replicationFactor defaulting to 3
    val actualReplicationFactor: Short = if (actualTopicConfig.contains(TopicConfigKeys.confluentPlacementConstraints)) -1 else topic.replicationFactor.getOrElse(3)

    KafkaClusterItem.Topic(
      topic.name,
      topic.partitions,
      actualReplicationFactor,
      actualTopicConfig,
      isReal = !topic.unManaged
    ).some
  }

  def describeAllTopics(
    hostOrNone: Option[String] = None,
    principal: String
  ) = Seq(allowTopic("*", principal, AclOperation.DESCRIBE, PatternType.LITERAL, hostOrNone))

  def connectTopicGroupAcls(
    hostOrNone: Option[String] = None,
    principal: String
  ) = Seq(
    allowTopic("connect-", principal, AclOperation.ALL, PatternType.PREFIXED, hostOrNone),
    allowGroup("connect-", principal, AclOperation.READ, PatternType.PREFIXED, hostOrNone)
  )

  def forApplication(
    application: TopologyToDeploy.Application,
    consumingApplication: Boolean,
    additionalUsers: Seq[String] = Seq.empty
  ): Seq[KafkaClusterItem] = {
    val host = application.host
    val users = Seq(application.user) ++ additionalUsers

    users.flatMap { user =>
      // consumer group permission (covers kafka-streams apps too)
      val consumerGroupAcl =
        if (consumingApplication || application.otherConsumableNamespaces.nonEmpty)
        // the ACL to read the application's consumer group is needed only if there is a consuming relationship for this application in this topology
        // OR this application is exposed to consume other topologies' topics
          application.actualConsumerGroup
            .map(consumerGroup => Seq(allowGroup(resourceName = consumerGroup, user, AclOperation.READ, PatternType.LITERAL, host)))
            .getOrElse(Seq.empty)
        else
          Seq.empty

      val transactionalIdAcls = {
        // the transaction id acl is prefixed so that kafka-streams and flink apps work (they typically allow to specify only a prefix for the transactional id they use, they generate the rest).
        application.actualTransactionalId
          .map(transactionalid => Seq(allowTransactionalId(resourceName = transactionalid, user, AclOperation.ALL, PatternType.PREFIXED, host)))
          .getOrElse(Seq.empty)
      }

      val kafkaStreamsAppAcls =
        application.typeAsKafkaStreams.map(_.kafkaStreamsAppId).map(kafkaStreamsAppId => {
          describeAllTopics(host, user) ++
            Seq(
              // create/delete topics with the kafkaStreamsAppId prefix
              allowTopic(resourceName = kafkaStreamsAppId, user, AclOperation.CREATE, PatternType.PREFIXED, host),
              allowTopic(resourceName = kafkaStreamsAppId, user, AclOperation.DELETE, PatternType.PREFIXED, host),
              // read/write/describe those topics
              allowTopic(resourceName = kafkaStreamsAppId, user, AclOperation.DESCRIBE, PatternType.PREFIXED, host),
              allowTopic(resourceName = kafkaStreamsAppId, user, AclOperation.READ, PatternType.PREFIXED, host),
              allowTopic(resourceName = kafkaStreamsAppId, user, AclOperation.WRITE, PatternType.PREFIXED, host),
              // idempotent write in case it needs
              allowCluster(user, AclOperation.IDEMPOTENT_WRITE, host)
            )
        }).getOrElse(Seq.empty)

      // extra acl for connect
      val connectorAcls =
        application.typeAsConnector.map(_.connector)
          .map(_ => describeAllTopics(host, user) ++ connectTopicGroupAcls(host, user)).getOrElse(Seq.empty)

      // extra acl for connect-replicator
      val connectReplicatorAcls =
        application.typeAsConnectReplicator.map(_.connectReplicator)
          .map(connectReplicator => {
            describeAllTopics(host, user) ++        // for some reason the replicator wants to list/describe all topics
              connectTopicGroupAcls(host, user) :+    // the usual connect- topic/group ACLs
              allowGroup(connectReplicator, user, AclOperation.READ, PatternType.LITERAL, host) :+   // the replicator wants to read (sometimes) the group of its name
              allowTopic("_confluent", user, AclOperation.ALL, PatternType.PREFIXED, host) // the replicator wants to read, possibly write _confluent topics sometimes
          }).getOrElse(Seq.empty)

      consumerGroupAcl ++ transactionalIdAcls ++ kafkaStreamsAppAcls ++ connectorAcls ++ connectReplicatorAcls
    }
  }

  def forConsume(
    ownerTopologyIds: Set[TopologyEntityId],
    user: String,
    hostOrNone: Option[String],
    topic: String
  ): Seq[KafkaClusterItemOfTopology] = {
    Seq(
      allowTopic(resourceName = topic, user, AclOperation.READ, PatternType.LITERAL, hostOrNone),
      allowTopic(resourceName = topic, user, AclOperation.DESCRIBE, PatternType.LITERAL, hostOrNone)
    ).map(KafkaClusterItemOfTopology(_, ownerTopologyIds))
  }

  def forProduce(
    ownerTopologyIds: Set[TopologyEntityId],
    user: String,
    hostOrNone: Option[String],
    topic: String
  ): Seq[KafkaClusterItemOfTopology] = {
    Seq(
      allowTopic(resourceName = topic, user, AclOperation.WRITE, PatternType.LITERAL, hostOrNone),
      allowTopic(resourceName = topic, user, AclOperation.DESCRIBE, PatternType.LITERAL, hostOrNone),
      allowCluster(user, AclOperation.IDEMPOTENT_WRITE, hostOrNone),
    ).map(KafkaClusterItemOfTopology(_, ownerTopologyIds))
  }

  def forApplicationTopicRelationship(
    additionalUsers: Seq[String] = Seq.empty
  )(
    relationship: TopologyToDeploy.ApplicationTopicRelationship
  ): Seq[KafkaClusterItemOfTopology] = {
    val ownerTopologyIds = Set(relationship.resolveNode1.topologyNode.topologyId, relationship.resolveNode2.topologyNode.topologyId)
    val applicationNode = relationship.resolveNode1.topologyNode.node
    val topicNode = relationship.resolveNode2.topologyNode.node
    val topicName = topicNode.name
    val host = applicationNode.host
    val users = Seq(applicationNode.user) ++ additionalUsers
    users.flatMap { user =>
      relationship.relationship match {
        case _: RelationshipType.Consume => forConsume(ownerTopologyIds, user, host, topicName)
        case _: RelationshipType.Produce => forProduce(ownerTopologyIds, user, host, topicName)
        case _ => Seq.empty
      }
    }
  }

  def forTopology(
    resolveTopicConfig: ResolveTopicConfig,
    allNodesMap: Map[NodeId, TopologyNode],
    topicDefaults: TopicDefaults
  )(
    topologyId: TopologyEntityId,
    topologyToDeploy: TopologyToDeploy
  ): Seq[KafkaClusterItemOfTopology] = {

    // this method assumes that the validation ensured that the topology and its dependencies is consistent and valid

    // resolving relationships
    val resolveFunc = resolveNodeRefFunc(allNodesMap, topologyToDeploy)
    val resolvedRelationships = collectAllVisibleRelationshipsWithoutErrorsFor(topologyId, topologyToDeploy, resolveFunc, topicDefaults)
    // need to collect only the relevant relationships of the resolved ones
    val resolvedApplicatinTopicRelationships = resolvedRelationships.flatMap(resolveToApplicationTopicRelationship(_))
    val relevantRelationships = resolvedApplicatinTopicRelationships.filter(_.relationship.isConsumeOrProduce)
    // we need to know what applications are consumers so that they will have group-read permission too
    val consumingApplicationIds = relevantRelationships.filter(_.relationship.isConsume).map(_.resolveNode1.topologyNode.nodeId).toSet

    // developer acls need to know the relationships (to generate acls for external ones)
    val developerAcls = forDevelopers(topologyId, topologyToDeploy, relevantRelationships, topicDefaults).map(KafkaClusterItemOfTopology(_, topologyId))

    // topics, acls, etc...
    val topics = topologyToDeploy.fullyQualifiedTopics.values.flatMap(forTopic(resolveTopicConfig)).map(KafkaClusterItemOfTopology(_, topologyId))

    // if the topology is in the root namespace (i.e. empty string), its developers will be additional users of all applications
    // this is like this to compensate for the lack of namespace prefix acls for developers (we don't want to give developers * permissions for everything,#
    // just because the topology doesn't have a namespace)
    val additionalApplicationDevelopers = if (topologyToDeploy.namespace.isEmpty) {
      topologyToDeploy.developers
    } else {
      Seq.empty
    }

    val consumerGroupAcls = topologyToDeploy.fullyQualifiedApplications
      .flatMap { case (id, a) => forApplication(a, consumingApplicationIds.contains(id), additionalApplicationDevelopers) }
      .map(KafkaClusterItemOfTopology(_, topologyId))

    developerAcls ++
      topics ++
      consumerGroupAcls ++
      relevantRelationships.flatMap(forApplicationTopicRelationship(additionalApplicationDevelopers))
  }

  def forAllTopologies(
    resolveTopicConfig: ResolveTopicConfig,
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    isKafkaClusterSecurityEnabled: Boolean,
    topicDefaults: TopicDefaults
  ): Seq[KafkaClusterItemOfTopology] = {
    val allNodesMap = allNodesMapOfTopologies(currentTopologies.values)

    val kafkaClusterItemsFunc = forTopology(resolveTopicConfig, allNodesMap, topicDefaults) _
    currentTopologies
      .flatMap { case (topologyId, topology) => kafkaClusterItemsFunc(topologyId, topology) }
      // we may need to filter out ACLs (if the cluster doesn't have security enabled)
      .filter(i => isKafkaClusterSecurityEnabled || !i.kafkaClusterItem.isInstanceOf[KafkaClusterItem.Acl])
      .groupBy(_.kafkaClusterItem)
      // We may have a kafka-cluster item multiple times here belonging to different topologies.
      // Aggregating these by KafkaClusterItem, and summing up all the topology-ids they belong to
      .map { case (kci, v) => KafkaClusterItemOfTopology(kci, v.map(_.ownerTopologyIds).reduce(_ ++ _)) }
      // Note that we don't do global ACL prefix-deduplication to keep things a bit simpler (prefix deduplication = removing ACLs which are covered by other prefixed ACLs)
      // In theory we could, but we'd need to be more careful: every removed ACL would result in the "parent" prefixed ACL getting the topology-ids of the removed one,
      // so that both topologies "depend" on the prefixed one and thus it gets created from both topologies.
      //
      // Having said all this, we don't do it, just to keep things simple.
      .toSeq
  }

  def forAllTopologies(
    resolveTopicConfig: ResolveTopicConfig,
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeploy: TopologyToDeploy,
    topicDefaults: TopicDefaults
  ): Seq[KafkaClusterItemOfTopology] =
    forAllTopologies(resolveTopicConfig, currentTopologies + (topologyId -> topologyToDeploy), isKafkaClusterSecurityEnabled, topicDefaults)
}
