/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import java.time.{Duration => JavaDuration}

import scala.collection.JavaConverters._
import cats.syntax.either._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.kafkacluster.KafkaCluster
import com.mwam.kafkakewl.domain.topology.{ApplicationId, TopologyToDeploy}
import com.mwam.kafkakewl.processor.kafkacluster.common.{KafkaConnectionOperations, KafkaConsumerExtraSyntax}
import io.circe.parser._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

trait ResetConnector {
  this: ResetCommon with KafkaConnectionOperations with KafkaConsumerExtraSyntax =>

  private def validateConnectorApplication(
    topologyToDeploy: TopologyToDeploy,
    application: ApplicationId,
  ): Result = {
    Validation.Result.validationErrorIf(
      topologyToDeploy.fullyQualifiedApplications.get(application).exists(_.typeAsConnector.isEmpty),
      s"application '$application' is not a connector application"
    )
  }

  private def validateConnectReplicatorApplication(
    topologyToDeploy: TopologyToDeploy,
    application: ApplicationId,
  ): Result = {
    Validation.Result.validationErrorIf(
      topologyToDeploy.fullyQualifiedApplications.get(application).exists(_.typeAsConnectReplicator.isEmpty),
      s"application '$application' is not a connect replicator application"
    )
  }

  private def produceEmptyMessages(
    producer: KafkaProducer[String, String],
    keys: Seq[String],
    connectOffsetTopic: String = "connect-offsets"
  )(implicit executionContext: ExecutionContextExecutor): ValueOrCommandErrors[Unit] = {
    Try {
      val produceResultFutures = keys.map(key => producer.produceString(connectOffsetTopic, key, ""))
      Await.result(Future.sequence(produceResultFutures), Duration.Inf)
      ()
    }.toEither.leftMap(t => Seq(CommandError.exception(t)))
  }


  def readConnectOffsetsMessages(
    consumer: KafkaConsumer[String, String],
    keyFilter: String => Boolean,
    connectOffsetTopic: String = "connect-offsets",
    pollTimeOutMillis: Int = 1000
  ): ValueOrCommandErrors[Seq[String]] = {
    Try {
      val connectorKeys = mutable.Set[String]()
      try {
        val topicPartitions = consumer.assignPartitionsOf(Set(connectOffsetTopic))
        val endOffsets = consumer.endOffsets(topicPartitions)
        consumer.seekToBeginning(topicPartitions)

        def shouldConsume(): Boolean = {
          consumer.assignedTopicPartitions
            .map(tp => tp -> (consumer.position(tp, JavaDuration.ofSeconds(10)) : scala.Long))
            .exists { case (tp, o) => o < endOffsets.getOrElse(tp, -1L) }
        }

        while (shouldConsume()) {
          val records = consumer.poll(JavaDuration.ofMillis(pollTimeOutMillis))
          for (record <- records.asScala) {
            val key = record.key()
            if (keyFilter(key)) {
              connectorKeys += key
            }
          }
        }
        connectorKeys.toSeq.sorted
      } finally {
        consumer.close()
      }
    }.toEither.leftMap(t => Seq(CommandError.exception(t)))
  }

  def resetConnectorApplication(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployedTopologyReset,
    topologyToDeploy: TopologyToDeploy,
    application: ApplicationId,
    options: ResetApplicationOptions.Connector
  )(implicit executionContext: ExecutionContextExecutor): Either[KafkaClusterCommandResult.Failed, KafkaClusterCommandResult.Succeeded] = {

    def keyFilter(connectorName: String)(key: String): Boolean = {
      val headString = for {
        jsonKey <- parse(key).toOption
        jsonArray <- jsonKey.asArray
        jsonArrayHead <- jsonArray.headOption
        jsonArrayHeadString <- jsonArrayHead.asString
      } yield jsonArrayHeadString
      headString.contains(connectorName)
    }

    val result = for {
      // basic validation
      _ <- Seq(
        validateApplication(topologyToDeploy, application),
        validateConnectorApplication(topologyToDeploy, application)
      ).combine().toCommandErrors

      // now it's safe to get the connector name
      connector = topologyToDeploy.fullyQualifiedApplications
        .get(application)
        .map(a => a.typeAsConnector.get.connector).get

      connectorKeys <- withStringConsumerAndCommandErrors(None)(readConnectOffsetsMessages(_, keyFilter(connector)))
      filteredConnectorKeys = options.keyRegex
        .map { keyRegex => connectorKeys.filter(_.matches(keyRegex)) }
        .getOrElse(connectorKeys)

      // checking for authorization code if we're about to perform any change
      _ <- failIfAuthorizationCodeNeeded(
        kafkaCluster,
        topologyToDeploy.deployWithAuthorizationCode,
        // this gets encoded in the authorization code, so that it can't be used for a different operation
        resetSummary = filteredConnectorKeys.sorted.mkString(","),
        command,
        filteredConnectorKeys.nonEmpty,
        command.options.authorizationCode
      )

      _ <- if (command.dryRun) ().asRight else withStringProducerAndCommandErrors(produceEmptyMessages(_, filteredConnectorKeys))
    } yield (connector, filteredConnectorKeys)

    result
      .map { case (connector, keys) => command.succeededResult(KafkaClusterCommandResponse.Connector(connector, keys)) }
      .leftMap(command.failedResult)
  }

  def resetConnectReplicatorApplication(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployedTopologyReset,
    topologyToDeploy: TopologyToDeploy,
    application: ApplicationId,
    options: ResetApplicationOptions.ConnectReplicator
  )(implicit executionContext: ExecutionContextExecutor): Either[KafkaClusterCommandResult.Failed, KafkaClusterCommandResult.Succeeded] = {

    def keyFilter(connectReplicator: String)(key: String): Boolean = {
      val matchingKey = for {
        // extracting the first item in the array, fail-fast
        jsonKey <- parse(key).toOption
        jsonArray <- jsonKey.asArray
        jsonArrayHead <- jsonArray.headOption
        jsonArrayHeadString <- jsonArrayHead.asString
      } yield {
        val keyConnectReplicator = jsonArrayHeadString

        val topicPartitionOrNone = for {
          jsonArrayTailHead <- jsonArray.tail.headOption
          jsonArrayTailHeadObject <- jsonArrayTailHead.asObject
          jsonArrayTailHeadObjectMap = jsonArrayTailHeadObject.toMap
          // extracting the topic (but no fail-fast)
          topic = for {
            topicJson <- jsonArrayTailHeadObjectMap.get("topic")
            topic <- topicJson.asString
          } yield topic
          // extracting the partition (but no fail-fast)
          partition = for {
            partitionJson <- jsonArrayTailHeadObjectMap.get("partition")
            partitionNumber <- partitionJson.asNumber
            partition <- partitionNumber.toInt
          } yield partition
        } yield (topic, partition)
        val topicOrNone = topicPartitionOrNone.flatMap(_._1)
        val partitionOrNone = topicPartitionOrNone.flatMap(_._2)

        // final condition, deciding whether this key matches our options or not
        keyConnectReplicator == connectReplicator &&
          options.topic.forall(topicOrNone.contains) &&
          options.partition.forall(partitionOrNone.contains)
      }
      // None matchingKey means no match
      matchingKey.getOrElse(false)
    }

    val result = for {
      // basic validation
      _ <- Seq(
        validateApplication(topologyToDeploy, application),
        validateConnectReplicatorApplication(topologyToDeploy, application)
      ).combine().toCommandErrors

      // now it's safe to get the connect replicator
      connectReplicator = topologyToDeploy.fullyQualifiedApplications
        .get(application)
        .map(a => a.typeAsConnectReplicator.get.connectReplicator).get

      connectorKeys <- withStringConsumerAndCommandErrors(None)(readConnectOffsetsMessages(_, keyFilter(connectReplicator)))

      // checking for authorization code if we're about to perform any change
      _ <- failIfAuthorizationCodeNeeded(
        kafkaCluster,
        topologyToDeploy.deployWithAuthorizationCode,
        // this gets encoded in the authorization code, so that it can't be used for a different operation
        resetSummary = connectorKeys.sorted.mkString(","),
        command,
        connectorKeys.nonEmpty,
        command.options.authorizationCode
      )

      _ <- if (command.dryRun) ().asRight else withStringProducerAndCommandErrors(produceEmptyMessages(_, connectorKeys))
    } yield (connectReplicator, connectorKeys)

    result
      .map { case (connectReplicator, keys) => command.succeededResult(KafkaClusterCommandResponse.ConnectReplicator(connectReplicator, keys)) }
      .leftMap(command.failedResult)
  }
}
