/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaAdminConfig, KafkaConfigProperties, KafkaConnection}
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.AdminClient

import scala.util.{Failure, Success}

class KafkaKewlTopicError(message: String) extends Exception(message)

final case class KafkaKewlSystemTopicConfig(
  replicationFactor: Short = 3,
  kafkaConfig: Map[String, String] = Map.empty
)

object KafkaKewlTopicUtils extends LazyLogging {
  private def envTopicFragment(env: Env): String = {
    env match {
      case Env.Prod => ""
      case e : Env => s".${e.toString.toLowerCase}"
    }
  }

  def changes(env: Env, states: String): String = s"kewl${envTopicFragment(env)}.changes.$states"

  val changeLog: String = s"kewl.changelog"

  def createIfNeeded(
    env: Env,
    connection: KafkaConnection,
    systemTopicConfig: KafkaKewlSystemTopicConfig,
    topics: Seq[String],
    wipeAndRecreate: Boolean,
    resolveTopicConfig: Map[String, String] => Map[String, String] = identity
  ): Unit = {
    val topicConfig = Map("retention.ms" -> "8640000000000") ++
      // if possible, setting the "min.insync.replicas" to a higher than default number
      (if (systemTopicConfig.replicationFactor >= 3) Map("min.insync.replicas" -> "2") else Map.empty) ++
      // the user defined kafka-config overrides everything
      systemTopicConfig.kafkaConfig

    // in case we need to modify the topic-config
    val resolvedTopicConfig = resolveTopicConfig(topicConfig)

    val admin = AdminClient.create(KafkaConfigProperties.forAdmin(KafkaAdminConfig(connection)))

    try {
      if (wipeAndRecreate) {
        topics
          .map(admin.deleteTopicIfExists)
          .foreach {
            case Success(r) => r.written.foreach(logger.info(_))
            case Failure(t) =>
              logger.error(t.toString)
              ApplicationMetrics.errorCounter.inc()

          }
        // just so that we wait for the topics to be properly and certainly deleted
        Thread.sleep(5000)
      }

      val results = topics
        .map(t => admin.createOrAlterTopic(t, partitions = 1, replicationFactor = systemTopicConfig.replicationFactor, config = resolvedTopicConfig))

      results
        .foreach {
          case Success(r) => r.written.foreach(logger.info(_))
          case Failure(t) =>
            logger.error(t.toString)
            ApplicationMetrics.errorCounter.inc()
        }

      val failures = results.collect { case Failure(t) => t }
      if (failures.nonEmpty) {
        // if there was any error, we need throw up and fail-fast
        throw new KafkaKewlTopicError(failures.map(t => t.toErrorMessage).mkString("\n"))
      }
    } finally {
      admin.close()
    }
  }
}
