/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers

package lag

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.gracefulStop
import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupMetricsObserver, ConsumerGroupTopicPartition}
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupOffset, PartitionLowHighOffset}
import com.mwam.kafkakewl.kafka.utils.KafkaConnection

import java.time.{Clock, OffsetDateTime}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.collection.{SortedMap, concurrent}
import scala.concurrent.Await
import scala.concurrent.duration._

private[lag] final case class ConsumerGroupMetricsConfig(slidingWindowSize: Option[Duration])

object ConsumerGroupStatusEvaluatorOfKafkaCluster {
  final case class Config(
    consumerGroupOffsetMode: ConsumerGroupOffsetMode,
    slidingWindowSizeFunc: ConsumerGroupTopicPartition => Duration,
    expectedWindowFillFactor: Double,
    consumeAfterConsumerGroupOffsetEnabled: Boolean,
    consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long,
    consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName],
    consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName],
    consumeAfterConsumerGroupOffsetNumberOfWorkers: Int,
    consumeAfterConsumerGroupOffsetPollDuration: java.time.Duration
  ) {
    def withConsumerGroupTopicPartition(consumerGroupTopicPartition: ConsumerGroupTopicPartition) =
      ConsumerGroupStatusEvaluatorConfig(
        consumerGroupOffsetMode,
        slidingWindowSizeFunc(consumerGroupTopicPartition),
        expectedWindowFillFactor
      )
  }
}

class ConsumerGroupStatusEvaluatorOfKafkaCluster(
  kafkaClusterId: KafkaClusterEntityId,
  consumerGroupMetricsObserver: ConsumerGroupMetricsObserver,
  config: ConsumerGroupStatusEvaluatorOfKafkaCluster.Config
)(implicit system: ActorSystem) {
  private val statusActors = concurrent.TrieMap.empty[ConsumerGroupTopicPartition, ActorRef]

  // this one is just updated/read from a single thread
  private var deployedTopologies = Map.empty[DeployedTopologyEntityId, DeployedTopology]
  // this one is updated from one thread and read from the others
  private val consumerGroupMetricsConfigs = new AtomicReference[Map[String, ConsumerGroupMetricsConfig]](Map.empty)

  // this actor can consume the kafka-topics a bit to confirm whether the lag is real
  private var consumeAfterConsumerGroupOffsetActor: Option[ActorRef] = none
  // to synchronize access to the consumeAfterConsumerGroupOffsetActor
  private val consumeAfterConsumerGroupOffsetActorLock: AnyRef = new Object()

  private def recreateTopicMetricsConfigs(): Unit = {
    val newConsumerGroupMetricsConfigs = deployedTopologies.values
      .flatMap(_.topologyWithVersion)
      .flatMap(_.topology.applications.values)
      .flatMap { application =>
        application.actualConsumerGroup.map { consumerGroup =>
          (consumerGroup, ConsumerGroupMetricsConfig(application.consumerLagWindowSeconds.map(Duration(_, TimeUnit.SECONDS))))
        }
      }
      .toMap

    // notifying existing actors about new state (even if there is no change)
    // note that if a deployed-topology is deleted, we don't send update, because it doesn't matter
    statusActors
      .foreach {
        case (consumerGroupTopicPartition, actorRef) => newConsumerGroupMetricsConfigs
          .get(consumerGroupTopicPartition.consumerGroupId)
          .foreach { consumerGroupMetricsConfig => actorRef ! consumerGroupMetricsConfig }
    }

    // consumerGroupMetricsConfigs may be read from different threads, technically a volatile would be enough too
    consumerGroupMetricsConfigs.set(newConsumerGroupMetricsConfigs)
  }

  private def createStatusActor(consumerGroupTopicPartition: ConsumerGroupTopicPartition, consumeAfterConsumerGroupOffsetActor: Option[ActorRef]): ActorRef = {
    val statusActor = system.actorOf(
      ConsumerGroupTopicPartitionStatusActor.props(
        kafkaClusterId,
        consumerGroupTopicPartition,
        config.withConsumerGroupTopicPartition(consumerGroupTopicPartition),
        consumerGroupMetricsObserver,
        config.consumeAfterConsumerGroupOffsetEnabled,
        config.consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix,
        config.consumeAfterConsumerGroupOffsetExcludeTopics,
        config.consumeAfterConsumerGroupOffsetExcludeConsumerGroups
      ),
      ConsumerGroupTopicPartitionStatusActor.name(kafkaClusterId, consumerGroupTopicPartition)
    )

    // if we have any topic metrics config, we send it now
    consumerGroupMetricsConfigs.get
      .get(consumerGroupTopicPartition.consumerGroupId)
      .foreach { consumerGroupMetricsConfig => statusActor ! consumerGroupMetricsConfig }

    // if we have any consumeAfterConsumerGroupOffsetActor, we send it now (we're within the consumeAfterConsumerGroupOffsetActorLock)
    consumeAfterConsumerGroupOffsetActor.foreach { statusActor ! ConsumerGroupTopicPartitionStatusActor.SetConsumeAfterConsumerGroupOffsetActor(_) }

    statusActor
  }

  def createOrGetStatusActor(consumerGroupTopicPartition: ConsumerGroupTopicPartition): ActorRef = {
    // just trying to get first without the consumeAfterConsumerGroupOffsetActorLock...
    statusActors.getOrElse(
      consumerGroupTopicPartition,
      {
        // ..., creation must be done properly within consumeAfterConsumerGroupOffsetActorLock so that we use the consumeAfterConsumerGroupOffsetActor and add to statusActors
        // atomically. This way we guarantee that we use the consumeAfterConsumerGroupOffsetActorLock either here or concurrently in the setKafkaConnection()
        consumeAfterConsumerGroupOffsetActorLock.synchronized {
          // we get/create/add within the lock to make sure that we
          statusActors.getOrElseUpdate(consumerGroupTopicPartition, createStatusActor(consumerGroupTopicPartition, consumeAfterConsumerGroupOffsetActor))
        }
      }
    )
  }

  private def removeStatusActor(consumerGroupTopicPartition: ConsumerGroupTopicPartition): Unit = {
    statusActors.get(consumerGroupTopicPartition).foreach { actor =>
      // graceful stop, waiting for the actor to be stopped so that it's more likely that its name is available in case we get an update quickly
      // TODO it's not guaranteed, what can we do?
      Await.ready(gracefulStop(actor, 1.hour), Duration.Inf)
      // even this is async, we don't really know when it actually is stopped
      statusActors.remove(consumerGroupTopicPartition)
    }
  }

  private def updateAllTopicInfos(timestampUtc: OffsetDateTime, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit = {
    // statusActors can be modified concurrently from updateAllConsumerGroupOffsets and updateConsumerGroupOffsets methods but that's fine,
    // that's why it's a concurrent TrieMap

    // if we run this logic based on a half-way modified statusActors, that's fine too, the next time we get topic-infos, we'll correct it anyway

    statusActors.toSeq.foreach { case (cgtp, actor) =>
      val lowHighOffsetOrNone = for {
        topicInfo <- allTopicInfos.get(cgtp.topicPartition.topic)
        partitionInfo <- topicInfo.partitions.get(cgtp.topicPartition.partition)
      } yield (partitionInfo.lowOffset, partitionInfo.highOffset)

      // if there is a low/high offset for this actor
      lowHighOffsetOrNone.foreach { case (lowOffset, highOffset) => actor ! PartitionLowHighOffset(lowOffset, highOffset, timestampUtc) }

      // this topic-partition doesn't exist anymore (TODO are we sure?)
      if (lowHighOffsetOrNone.isEmpty) {
        removeStatusActor(cgtp)
      }
    }
  }

  def updateAllTopicInfos(allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit = updateAllTopicInfos(OffsetDateTime.now(Clock.systemUTC()), allTopicInfos)

  def updateAllConsumerGroupOffsets(offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset]): Unit = {
    //logger.info(s"updateAllConsumerGroupOffsets(): offsets(${offsets.size}) = ${offsets.map { case (k, v) => s"$k:$v" }.mkString("; ")}")

    // this method is NOT called concurrently with updateConsumerGroupOffsets so the line below is fine (the actor is created ONLY once)
    offsets.foreach { case (cgtp, cgo) =>
      val actor = createOrGetStatusActor(cgtp)
      actor ! cgo
    }

    // removing the actors that are not in offsets
    val actorKeysNotInOffsets = statusActors.keySet -- offsets.keys
    actorKeysNotInOffsets.foreach(removeStatusActor)

    // notifying the observers about those consumer group metrics being removed
    actorKeysNotInOffsets.foreach(consumerGroupMetricsObserver.removeConsumerGroupMetrics(kafkaClusterId, _))
  }

  def updateConsumerGroupOffsets(offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset], removedOffsets: Iterable[ConsumerGroupTopicPartition]): Unit = {
    if (offsets.nonEmpty) {
      //logger.info(s"updateConsumerGroupOffsets(): offsets(${offsets.size}) = ${offsets.map { case (k, v) => s"$k:$v" }.mkString("; ")}")
    }
    if (removedOffsets.nonEmpty) {
      //logger.info(s"updateConsumerGroupOffsets(): removed(${removedOffsets.size}) = ${removedOffsets.map(_.toString).mkString("; ")}")
    }

    // this method is NOT called concurrently with updateAllConsumerGroupOffsets so the line below is fine (the actor is created ONLY once)
    offsets.foreach { case (cgtp, cgo) =>
      val actor = createOrGetStatusActor(cgtp)
      actor ! cgo
    }

    // removing the actors that are removed
    removedOffsets.foreach(removeStatusActor)

    // notifying the observers about those consumer group metrics being removed
    removedOffsets.foreach(consumerGroupMetricsObserver.removeConsumerGroupMetrics(kafkaClusterId, _))
  }

  def createOrUpdateDeployedTopology(deployedTopologyId: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit = {
    // createOrUpdateDeployedTopology and removeDeployedTopology are called from the same thread, no need for sync
    deployedTopologies += (deployedTopologyId -> deployedTopology)
    recreateTopicMetricsConfigs()
  }

  def removeDeployedTopology(deployedTopologyId: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit = {
    // createOrUpdateDeployedTopology and removeDeployedTopology are called from the same thread, no need for sync
    deployedTopologies -= deployedTopologyId
    recreateTopicMetricsConfigs()
  }

  /**
   * Starts using the specified kafka-connection.
   *
   * This can be called concurrently to any other method except the removeKafkaConnection().
   *
   * @param kafkaConnection the kafka-connection.
   */
  def setKafkaConnection(kafkaConnection: KafkaConnection): Unit = {
    consumeAfterConsumerGroupOffsetActorLock.synchronized {
      this.consumeAfterConsumerGroupOffsetActor match {
        case Some(actor) => actor
        case None =>
          // need to create it first
          val actor = system.actorOf(
            ConsumeAfterConsumerGroupOffsetRouterActor.props(
              kafkaClusterId,
              config.consumeAfterConsumerGroupOffsetNumberOfWorkers,
              config.consumeAfterConsumerGroupOffsetPollDuration
            ),
            ConsumeAfterConsumerGroupOffsetRouterActor.name(kafkaClusterId)
          )
          actor ! ConsumeAfterConsumerGroupOffsetActor.SetKafkaConnection(kafkaConnection)
          // broadcast the newly created ConsumeAfterConsumerGroupOffsetActor to all status-actors
          statusActors.foreach { case (_, statusActor) => statusActor ! ConsumerGroupTopicPartitionStatusActor.SetConsumeAfterConsumerGroupOffsetActor(actor) }

          this.consumeAfterConsumerGroupOffsetActor = actor.some
      }
    }
  }

  /**
   * Stops using the current kafka-connection.
   *
   * This can be called concurrently to any other method except the setKafkaConnection().
   */
  def removeKafkaConnection(): Unit = {
    consumeAfterConsumerGroupOffsetActorLock.synchronized {
      this.consumeAfterConsumerGroupOffsetActor.foreach(_ ! PoisonPill)
      // no need to broadcast anything, the status-actors watch for the termination of the ConsumeAfterConsumerGroupOffsetActor
      this.consumeAfterConsumerGroupOffsetActor = none
    }
  }
}
