/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster

import cats.instances.vector._
import cats.syntax.either._
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.kafka.utils._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaAdminConfig, KafkaConfigProperties, KafkaConnection, KafkaOperationResult}
import com.mwam.kafkakewl.domain.CommandError
import com.mwam.kafkakewl.processor.kafkacluster.deployment.{KafkaClusterChange, KafkaClusterItem}
import org.apache.kafka.clients.admin.{AdminClient, Config, TopicDescription}
import org.apache.kafka.common.acl.AclBinding
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.OffsetOfTopicPartition
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys
import com.mwam.kafkakewl.domain.kafka.{KafkaConsumerGroup, KafkaTopic}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.processor.kafkacluster.common._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

final case class TopicConsumerGroups(topic: String, numberOfPartitions: Int, consumerGroups: Seq[String])

/**
  * Trait for high level kafka cluster admin APIs.
  */
trait KafkaClusterAdmin {
  /**
    * Loads all the kafka-cluster items (topics and acls) from the underlying kafka-cluster
    *
    * @return the kafka-cluster items or an exception
    */
  def kafkaClusterItems(): Try[Seq[KafkaClusterItem]]

  /**
    * True if security (ACLs) are enabled in the kafka-cluster.
    *
    * @return true if security (ACLs) are enabled in the kafka-cluster
    */
  def isSecurityEnabled: Boolean

  /**
    * Deploys the specified kafka-cluster change or does nothing in case of dry-run.
    *
    * @param change the change to be deployed
    * @param dryRun true if dry-run
    * @return the result of the deployment or an exception
    */
  def deployKafkaClusterChange(change: KafkaClusterChange, dryRun: Boolean): Try[KafkaOperationResult[Unit]]

  /**
    * Checks whether the specified topics can be deleted, if they can be prepares them for that which means
    * resetting impacted consumer groups' offsets (because otherwise kafka may or may not preserve those and
    * we can end up with zombie consumer groups with some old offsets).
    *
    * @param topicNames the names of the topics to prepare
    * @param topicsToRecreateResetGroups true if the non-live consumer groups must be reset for the topics that are deleted for re-creation.
    * @param dryRun true if dry-run
    * @return either the errors or the (topic, consumer groups) tuple's reset results
    */
  def prepareTopicsToBeDeleted(topicNames: Seq[String], topicsToRecreateResetGroups: Boolean = true, dryRun: Boolean): ValueOrCommandErrors[Map[(String, String), ValueOrCommandErrors[Seq[OffsetOfTopicPartition]]]]

  /**
    * Returns the basic topic info (low, high)
    *
    * @param topicName the name of the topic
    * @return the basic topic info
    */
  def getTopicInfo(topicName: String): ValueOrCommandErrors[KafkaTopic.Info]

  /**
    * Returns the basic consumer group info (offset lag, etc...)
    *
    * @param consumerGroupId the consumer group
    * @return the consumer group info
    */
  def getConsumerGroupInfo(consumerGroupId: String): ValueOrCommandErrors[KafkaConsumerGroup.Info]
}

class KafkaTopicNotFoundError(topic: String)
  extends RuntimeException(s"topic $topic not found.") {}
class KafkaTopicsNotFoundError(topics: Seq[String])
  extends RuntimeException(s"topics ${topics.mkString(", ")} not found.") {}
class KafkaTopicPartitionsDifferent(topic: String, before: Int, after: Int)
  extends RuntimeException(s"topic $topic's partitions cannot be updated from $before to $after.") {}
class KafkaTopicReplicationFactorDifferent(topic: String, before: Short, after: Short)
  extends RuntimeException(s"topic $topic's replication factor cannot be updated from $before to $after.") {}
class KafkaTopicConfigUpdateInvalid(topic: String, before: Map[String, String], after: Map[String, String])
  extends RuntimeException(s"topic $topic's config cannot be updated from $before to $after. Deleting config keys is not supported at the moment.") {}
class KafkaTopicPendingDeleted(topic: String)
  extends RuntimeException(s"topic $topic is being deleted (among '/admin/delete_topics' in ZK), cannot create it yet.") {}

/**
  * Default implementation of the KafkaClusterAdmin trait
  *
  * @param connection the kafka connection
  */
private[kafkacluster] class DefaultKafkaClusterAdmin(
  val kafkaClusterId: KafkaClusterEntityId,
  val connection: KafkaConnection,
  val kafkaCluster: KafkaCluster
) extends KafkaClusterAdmin
    with KafkaAdminClientOperations
    with KafkaAdminClientAndConsumerOperations[Array[Byte], Array[Byte]]
    with KafkaConnectionOperations
    with KafkaConsumerOperations[Array[Byte], Array[Byte]]
    with KafkaConsumerExtraSyntax
    with KafkaAdminClientExtraSyntax
    with DefaultInstrumented
    with LazyLogging {

  override lazy val metricBaseName: MetricName = MetricName("com.mwam.kafkakewl.processor.kafkacluster")

  val adminClient = AdminClient.create(KafkaConfigProperties.forAdmin(KafkaAdminConfig(connection, kafkaCluster.kafkaRequestTimeOutMillis)))
  val consumer = createConsumer(groupId = None, kafkaCluster.kafkaRequestTimeOutMillis)
  val zkClientOption = kafkaCluster.zooKeeper.map { zooKeepers =>
    val zkClient = CuratorFrameworkFactory.newClient(zooKeepers, new RetryForever(kafkaCluster.kafkaRequestTimeOutMillis.getOrElse(3000)))
    zkClient.start()
    zkClient
  }

  /**
    * Creates KafkaClusterItems for topics and ACLs in the kafka-cluster
    *
    * @return the KafkaClusterItems for topics and ACLs in the kafka-cluster
    */
  def kafkaClusterItems(): Try[Seq[KafkaClusterItem]] = {
    for {
      topicNames <- adminClient.topicNames()
      topicDescriptions <- adminClient.describeTopics(topicNames)
      _ <- {
        val missingTopicNames = topicNames.filter(topic => !topicDescriptions.contains(topic))
        if (missingTopicNames.isEmpty) Success(()) else Failure(new KafkaTopicsNotFoundError(missingTopicNames))
      }
      aclBindings <- adminClient.describeAcls()
    } yield topicDescriptions.values.map(DefaultKafkaClusterAdmin.kafkaClusterItem).toSeq ++ aclBindings.map(DefaultKafkaClusterAdmin.kafkaClusterItem)
  }

  def isSecurityEnabled: Boolean = adminClient.isSecurityEnabled

  def ensureNumberOfPartitionsIsTheSame(before: KafkaClusterItem.Topic, after: KafkaClusterItem.Topic): Try[Unit] = {
    if (before.partitions != after.partitions) Failure(new KafkaTopicPartitionsDifferent(before.name, before.partitions, after.partitions))
    else Success(())
  }

  def ensureReplicationFactorIsTheSame(before: KafkaClusterItem.Topic, after: KafkaClusterItem.Topic): Try[Unit] = {
    if (before.replicationFactor != after.replicationFactor && after.replicationFactor > 0)
      Failure(new KafkaTopicReplicationFactorDifferent(before.name, before.replicationFactor, after.replicationFactor))
    else
      // afterReplicationFactor = -1 is accepted even if it's different from beforeReplicationFactor: that just means adding replicaPlacement to the topic
      Success(())
  }

  def deployKafkaClusterChange(change: KafkaClusterChange, dryRun: Boolean): Try[KafkaOperationResult[Unit]] = {
    def ifNotDryRun(r: => Try[KafkaOperationResult[Unit]]): Try[KafkaOperationResult[Unit]] =
      if (!dryRun) r else Try(KafkaOperationResult.tell("dryRun=true: not done"))

    change match {
      case KafkaClusterChange.Add(topic: KafkaClusterItem.Topic) =>
        assert(topic.isReal, "cannot deploy (add) un-real topic")
        for { // based on Try
          deleteTopicsCheckResult <- Try {
            // If there is no zookeeper configured for this cluster, we just pretend that there is nothing in '/admin/delete_topics'
            Try(zkClientOption.map(_.getChildren.forPath("/admin/delete_topics").asScala.toList).getOrElse(Nil)) match {
              case Failure(t) =>
                val errorMessage = s"could not check the '/admin/delete_topics' node in ZK before creating the topic '${topic.name}': ${t.getMessage}"
                // I want to log this as an error with a specific message here (it'll be logged later as part of the deployment result too, but it's easier to find it if we log as a separate error)
                logger.error(errorMessage)
                metrics.counter(s"zk_get_deleted_topics_failed:$kafkaClusterId").inc()
                // We go ahead with the topic creation, it's probably better than not even trying. I'm not sure how often and when exactly
                // we can get into this code branch, we'll see if this needs to change.
                KafkaOperationResult.tell(errorMessage)
              case Success(deleteTopics) =>
                if (deleteTopics.contains(topic.name)) {
                  val exception = new KafkaTopicPendingDeleted(topic.name)
                  // I want to log this as an error with a specific message here (it'll be logged later as part of the deployment result too, but it's easier to find it if we log as a separate error)
                  logger.error(exception.getMessage)
                  metrics.counter(s"not_created_pending_deleted_topic:$kafkaClusterId:${topic.name}").inc()
                  // the outer Try ensures that we fail-fast before attempting to create the topic
                  throw exception
                } else {
                  KafkaOperationResult.empty
                }
            }
          }
          createTopicResult <- ifNotDryRun {
            adminClient.createTopic(topic.name, topic.partitions, topic.replicationFactor, topic.config)
          }
        } yield {
          // combining the 2 KafkaOperationResults' logs (they have no values)
          for { // based on KafkaOperationResult = Writer[Vector[String], _]
            _ <- deleteTopicsCheckResult
            _ <- createTopicResult
          } yield ()
        }

      case KafkaClusterChange.Add(acl: KafkaClusterItem.Acl) =>
        ifNotDryRun {
          adminClient.createAcl(acl.resourceType, acl.resourcePatternType, acl.resourceName, acl.principal, acl.host, acl.operation, acl.permission)
        }

      case KafkaClusterChange.Remove(topic: KafkaClusterItem.Topic) =>
        assert(topic.isReal, "cannot deploy (remove) un-real topic")
        ifNotDryRun {
          adminClient.deleteTopicIfExists(topic.name)
        }

      case KafkaClusterChange.Remove(acl: KafkaClusterItem.Acl) =>
        ifNotDryRun {
          adminClient.deleteAcl(acl.resourceType, acl.resourcePatternType, acl.resourceName, acl.principal, acl.host, acl.operation, acl.permission)
        }

      case KafkaClusterChange.UpdateTopic(beforeTopic: KafkaClusterItem.Topic, afterTopic: KafkaClusterItem.Topic) =>
        assert(afterTopic.isReal, "cannot deploy (update) un-real topic")
        ifNotDryRun {
          for {
            // Here we can't just update the topic's partitions or replication-factor. There are specific commands to do that, explicitly.
            _ <- ensureNumberOfPartitionsIsTheSame(beforeTopic, afterTopic)
            _ <- ensureReplicationFactorIsTheSame(beforeTopic, afterTopic)
            alterTopicResult <- if (beforeTopic.config != afterTopic.config) {
              adminClient.alterTopicConfig(afterTopic.name, afterTopic.config)
            } else
              Success(KafkaOperationResult.empty)
          } yield alterTopicResult
        }
    }
  }

  def prepareTopicsToBeDeleted(topicNames: Seq[String], topicsToRecreateResetGroups: Boolean = true, dryRun: Boolean): ValueOrCommandErrors[Map[(String, String), ValueOrCommandErrors[Seq[OffsetOfTopicPartition]]]] = {
    // getting the number of partitions for the topics (checking whether they exist at all)
    val topicPartitionsOrErrors = topicNames.map(topic => (topic, adminClient.numberOfTopicPartitions(topic).toRightOrCommandErrors))
    val topicPartitionsErrors = topicPartitionsOrErrors.collect { case (topic, Left(ce)) => ce.map(_.mapMessage(m => s"failed to prepare topic ${topic.quote} before deleting: $m")) }.flatten

    for {
      // step #0: initial information-gathering for consumer groups in the kafka-cluster - simple fail-fast behaviour in case of any error

      // TODO strictly speaking we wouldn't need to fail-fast here if a topic cannot be described - but it's easier slightly
      _ <- Either.cond(topicPartitionsErrors.isEmpty, (), topicPartitionsErrors)
      // if a topic doesn't exist (totally valid), we remove it from the set we're interested in and don't even return anything to it
      // (nothing to do to prepare this topic anyway)
      topicPartitions = topicPartitionsOrErrors.collect { case (topic, Right(Some(np))) => (topic, np) }.toMap
      topicNamesSet = topicPartitions.keySet

      // getting the list of ALL consumer groups
      consumerGroupListings <- adminClient.allConsumerGroups().toRightOrCommandErrors
      consumerGroups = consumerGroupListings.map(_.groupId)
      // ...and descriptions
      consumerGroupsDescriptions <- adminClient.describeConsumerGroups(consumerGroups).toRightOrCommandErrors
      // ... and offsets
      consumerGroupsOffsetsOrErrors = consumerGroups.map(cg => (cg, adminClient.consumerGroupOffsets(cg)))
      consumerGroupsOffsetsErrors = consumerGroupsOffsetsOrErrors
        .collect { case (cg, Failure(e)) => CommandError.otherError(s"could not retrieve the consumer group offsets for '{$cg': ${e.getMessage}") }
        .toSeq
      _ <- Either.cond(consumerGroupsOffsetsErrors.isEmpty, (), consumerGroupsOffsetsErrors)
      consumerGroupsTopicPartitionsWithOffsets = consumerGroupsOffsetsOrErrors
        .collect { case (cg, Success(cgo)) => cgo.keys.map(tpo => (cg, tpo.topic)).toSeq.distinct }
        .flatten
        .groupBy { case (cg, _) => cg }
        .mapValues(_.map { case (_, topic) => topic })
    } yield {
      // no fail-fast behaviour from now on, every error will be associated with a topic and that's what we return ultimately

      // step #1: filtering to the current topics
      val topicsConsumerGroupIdsWithMembers =
        (for {
          consumerGroupDescription <- consumerGroupsDescriptions.values
          memberDescription <- consumerGroupDescription.members().asScala
          topicPartition <- memberDescription.assignment().topicPartitions().asScala
          // filtering for current consumers for these topics
          if topicNamesSet(topicPartition.topic)
        } yield (consumerGroupDescription.groupId, topicPartition.topic))
          .toSeq.distinct
          .groupBy { case (cg, _) => cg }
          .mapValues(_.map { case (_, topicName) => topicName })

      val topicsConsumerGroupsTopicPartitionsWithOffsets = consumerGroupsTopicPartitionsWithOffsets
        .mapValues(_.filter(topicNamesSet))
        .filter { case (_, tps) => tps.nonEmpty }

      // step #2: consumer group liveness check of the consumer groups with these topic members
      val topicConsumerGroupWithMembersLivenessResults = topicsConsumerGroupIdsWithMembers.keys
        .map { cg => (cg, ensureConsumerGroupDescriptionIsNotLive(consumerGroupsDescriptions(cg)).left.map(e => Seq(CommandError.otherError(e)))) }
        .toMap
      val topicConsumerGroupWithMembersLive = topicConsumerGroupWithMembersLivenessResults.collect { case (cg, Left(_)) => cg }.toSet

      // step #3: resetting the consumer groups...
      val topicsConsumerGroupsResetResult = topicsConsumerGroupsTopicPartitionsWithOffsets
        // except the ones which are live
        .filterKeys(!topicConsumerGroupWithMembersLive(_))
        .flatMap {
          case (consumerGroupId, topics) =>
            // need a consumer with this consumer group
            val topicsResetResult = withConsumerAndCommandErrors(Some(consumerGroupId)) { consumer =>
              // resetting all topics' all partitions belonging to this consumer group
              topics.map { topic =>
                val topicPartitionOffsets = (0 until topicPartitions(topic)).map(OffsetOfTopicPartition(topic, _, 0L))
                (
                  topic,
                  // TODO this has problems: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records.
                  if (!dryRun && topicsToRecreateResetGroups) consumer.resetConsumerGroupOffsets(topicPartitionOffsets).map(_.toSeq) else topicPartitionOffsets.asRight
                )
              }.toMap.asRight[Seq[CommandError]]
            }
            // if the consumer failed, we treat that error as if all topics' resets failed with the same error message
            val topicsResetResults = topicsResetResult
              .leftMap(ce => topics.map((_, ce.asLeft)).toMap)
              .merge

            // the result, keyed by the consumerGroupId - topic tuple
            topicsResetResults.map { case (topic, rr) => ((topic, consumerGroupId), rr) }
        }

      // step #4: putting together the result: map by (topic-name - consumer group) tuple
      // need to combine the live-check results with the reset results
      val topicConsumerGroupLivenessResultsByTopicConsumerGroup =
        topicConsumerGroupWithMembersLivenessResults.collect {
          case (consumerGroupId, Left(ce)) =>
            // find the topics that were LIVE-consumed by this consumer group
            val liveConsumedTopics = topicsConsumerGroupIdsWithMembers(consumerGroupId)
            liveConsumedTopics
              .map(t => ((t, consumerGroupId), ce.asLeft[Seq[OffsetOfTopicPartition]]))
        }.flatten.toMap

      // there shouldn't be overlapping keys between these 2 maps: we reset only for the groups that are
      // successful in topicConsumerGroupLivenessResults
      // (topicConsumerGroupLivenessResultsByTopicConsumerGroup contains only failed ones)
      topicConsumerGroupLivenessResultsByTopicConsumerGroup ++ topicsConsumerGroupsResetResult
    }
  }

  def close(): Unit = {
    adminClient.close()
    consumer.close()
    zkClientOption.foreach(_.close())
  }
}

private[kafkacluster] object DefaultKafkaClusterAdmin {
  def kafkaClusterItem(kafkaTopicDetails: (TopicDescription, Config)): KafkaClusterItem = {
    val (kafkaTopicDescription, kafkaTopicConfig) = kafkaTopicDetails
    val topicConfig = kafkaTopicConfig.entries().asScala.map(e => (e.name(), e.value())).toMap
    val confluentPlacementConstraints = topicConfig.get(TopicConfigKeys.confluentPlacementConstraints)
    val partitionNoOfReplicas = kafkaTopicDescription.partitions().get(0).replicas().size().toShort
    // if the topic has replica-placements, the replicationFactor should be -1 so that it matches up with the one generated from the topologies
    // (and kafka itself wants -1 when creates a topic with replica-placements)
    val replicationFactor: Short = if (confluentPlacementConstraints.nonEmpty) -1 else partitionNoOfReplicas
    KafkaClusterItem.Topic(
      kafkaTopicDescription.name(),
      kafkaTopicDescription.partitions().size(),
      replicationFactor,
      topicConfig,
      isReal = true
    )
  }

  def kafkaClusterItem(aclBinding: AclBinding): KafkaClusterItem = {
    KafkaClusterItem.Acl(
      aclBinding.pattern().resourceType(),
      aclBinding.pattern().patternType(),
      aclBinding.pattern().name(),
      aclBinding.entry().principal(),
      aclBinding.entry().host(),
      aclBinding.entry().operation(),
      aclBinding.entry().permissionType(),
    )
  }
}
