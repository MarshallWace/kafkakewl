/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.HttpOrigin
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.codahale.metrics.jmx.JmxReporter
import com.mwam.kafkakewl.api.metrics.kafkacluster.groups.collectors.{KafkaClusterConsumerGroupOffsetsConsumer, KafkaClusterConsumerGroupOffsetsPoller}
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.{ConsumerGroupOffsetMode, ConsumerGroupStatusEvaluator, ConsumerGroupStatusEvaluatorOfKafkaCluster}
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.{ConsumerGroupMetricsCacheImpl, TopicInfoCacheImpl, TopicMetricsCacheImpl}
import com.mwam.kafkakewl.api.metrics.kafkacluster.topics.KafkaTopicMetricsPoller
import com.mwam.kafkakewl.api.metrics.kafkacluster.{KafkaClusterMetricsCollector, KafkaClusterMetricsCollectors, KafkaClusterObserver, TopicInfoObserver}
import com.mwam.kafkakewl.api.metrics.routes.{HealthRoute, KafkaClusterRoute}
import com.mwam.kafkakewl.api.metrics.services.KafkaClusterServiceImpl
import com.mwam.kafkakewl.api.metrics.state.KafkaChangeLogStoreConsumer
import com.mwam.kafkakewl.common.PluginLoader
import com.mwam.kafkakewl.common.http.{CORSHandler, HttpExtensions}
import com.mwam.kafkakewl.extensions.AuthPlugin
import com.mwam.kafkakewl.utils.ApplicationMetrics
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.DefaultInstrumented

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}

object HttpServerApp extends App
  with LazyLogging
  with HttpServerAppConfigProvider
  with CORSHandler
  with DefaultInstrumented {

  val allAllowedOrigins = httpAllowedOrigins.map(HttpOrigin(_))

  implicit val system: ActorSystem = ActorSystem("kafkakewl-api-metrics")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = 10.seconds // short http time-out, we'll serve quick GET requests after all

  val jmpReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("kafkakewl-api-metrics").build
  jmpReporter.start()

  ApplicationMetrics.initialize()

  val authPlugins = PluginLoader.loadPlugins[AuthPlugin]()

  logger.info(s"available processors:     ${Runtime.getRuntime.availableProcessors()}")
  logger.info(s"task-support parallelism: ${Seq.fill(50)(1).par.tasksupport.parallelismLevel}")
  logger.info(s"http hosting port:        $httpPort")
  if (httpAllowedOrigins.nonEmpty) {
    for (httpAllowedOrigin <- httpAllowedOrigins) {
      logger.info(s"http allowed origin:      $httpAllowedOrigin")
    }
  } else {
    logger.info(s"http allowed origins:     none")
  }

  logger.info(s"available auth plugins:   ${authPlugins.keys.mkString(", ")}")
  logger.info(s"chosen auth plugin:       $authPluginName")

  val authDirective = authPlugins
    .getOrElse(authPluginName, sys.error(s"couldn't find authentication plugin '$authPluginName'. Available plugins: ${authPlugins.keys.mkString(", ")}"))
    .createAuthenticationDirective(config)

  // caches, metrics collectors
  val topicInfoStateCache = new TopicInfoCacheImpl()
  val topicMetricsCache = new TopicMetricsCacheImpl(windowDuration = 60.seconds)
  val consumerGroupMetricsCache = new ConsumerGroupMetricsCacheImpl()

  val consumerGroupStatusEvaluator = new ConsumerGroupStatusEvaluator(
    consumerGroupMetricsCache,
    kid => ConsumerGroupStatusEvaluatorOfKafkaCluster.Config(
      consumerGroupOffsetMode,
      slidingWindowSizeFunc = _ => defaultLagWindowMinutes.minutes,
      expectedWindowFillFactor = defaultLagWindowFillFactorMinutes,
      consumeAfterConsumerGroupOffsetEnabled,
      consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix,
      consumeAfterConsumerGroupOffsetExcludeTopics,
      consumeAfterConsumerGroupOffsetExcludeConsumerGroups,
      consumeAfterConsumerGroupOffsetNumberOfWorkers,
      java.time.Duration.ofMillis(consumeAfterConsumerGroupOffsetPollDurationMillis)
    )
  )

  // the metrics collectors instance is responsible for collecting metrics across all clusters and reacting to cluster changes
  val kafkaClusterMetricsCollectors = new KafkaClusterMetricsCollectors(
    KafkaClusterMetricsCollector.Observers(
      KafkaClusterObserver.composite(topicInfoStateCache, topicMetricsCache, consumerGroupStatusEvaluator),
      TopicInfoObserver.composite(topicInfoStateCache, topicMetricsCache, consumerGroupStatusEvaluator),
      consumerGroupStatusEvaluator
    ),
    KafkaClusterMetricsCollector.Config(
      (helper, observers) => new KafkaTopicMetricsPoller(
        helper,
        observers,
        delayMillisBetweenTopicInfoPolls,
        !disabledKafkaClusterIdsForTopicInfoPolls.contains(helper.kafkaClusterId)
      )(pollingExecutionContext),
      consumerGroupOffsetMode match {
        case ConsumerGroupOffsetMode.ActualCommit =>
          (helper, observers) => new KafkaClusterConsumerGroupOffsetsConsumer(
            helper,
            observers,
            !disabledKafkaClusterIdsForConsumerGroupOffsets.contains(helper.kafkaClusterId),
            noOfThreadsForInitialLoad = threadsForInitialLoadForConsumerGroupConsumption,
            noOfThreadsForLiveConsumption = threadsForLiveConsumptionForConsumerGroupConsumption,
            compactionInterval = compactionDurationForConsumerGroupConsumption,
            excludeConsumerGroups = excludedConsumerGroupRegexesForConsumerGroupOffsets,
            consumerGroup = consumerGroupForConsumerGroupConsumption,
            consumerOffsetsTopicName = topicForConsumerGroupConsumption
          )
        case ConsumerGroupOffsetMode.Sampled =>
          (helper, observers) => new KafkaClusterConsumerGroupOffsetsPoller(
            helper,
            observers,
            delayMillisBetweenConsumerGroupOffsetsPolls,
            !disabledKafkaClusterIdsForConsumerGroupOffsets.contains(helper.kafkaClusterId),
            excludeConsumerGroups = excludedConsumerGroupRegexesForConsumerGroupOffsets,
          )(pollingExecutionContext)
      }
    )
  )

  // the services behind the routes
  val kafkaClusterService = new KafkaClusterServiceImpl(topicInfoStateCache, topicMetricsCache, consumerGroupMetricsCache)

  // need to consume the kafkakewl changelog to keep track of the current set of clusters
  val kafkaChangeLogConsumer =
    if (kafkaChangeLogStoreEnabled) {
      Some(
        new KafkaChangeLogStoreConsumer(
          kafkaChangeLogStoreConnection,
          // maintaining the metrics collectors for the current set of clusters
          KafkaChangeLogStoreConsumer.KafkaClusterObserverFunctions(
            (id, newKafkaCluster) => {
              val kafkaConnection = kafkaConnectionFrom(newKafkaCluster)
              // first make sure that the consumer group evaluator knows about this new kafka-cluster...
              consumerGroupStatusEvaluator.kafkaClusterCreated(id, kafkaConnection)
              // ... then we can tell the metrics-collectors too (so that by the time the consumer group evaluator receives lag information,
              // it knows about the kafka-cluster and can consume it to confirm the possible fake lags)
              kafkaClusterMetricsCollectors.createCollector(id, kafkaConnection)
            },
            (id, _, newKafkaCluster) => {
              val kafkaConnection = kafkaConnectionFrom(newKafkaCluster)
              // the order here is arbitrary, there is no good or bad order - either way things can be out-of-sync when the consumer group evaluator
              // wants to talk to the kafka-cluster, but we just need to handle errors and that's it.
              consumerGroupStatusEvaluator.kafkaClusterUpdated(id, kafkaConnection)
              kafkaClusterMetricsCollectors.updateCollector(id, kafkaConnection)
            },
            (id, _) => {
              // it's better to remove it first from the metrics-collectors so that they stop producing topic/consumer group metrics...
              kafkaClusterMetricsCollectors.removeCollector(id)
              // ...and then remove from the consumer group evaluator (it's still possible that we remove it while it's needed there, but we just need to handle errors)
              consumerGroupStatusEvaluator.kafkaClusterDeleted(id)
            },
          ),
          // notifying the lag evaluators about deployed-topology changes so that they can use topology attributes (e.g. the window size)
          KafkaChangeLogStoreConsumer.DeployedTopologyObserverFunctions(
            (id, newDeployedTopology) => consumerGroupStatusEvaluator.createOrUpdateDeployedTopology(id, newDeployedTopology),
            (id, _, newDeployedTopology) => consumerGroupStatusEvaluator.createOrUpdateDeployedTopology(id, newDeployedTopology),
            (id, deployedTopology) => consumerGroupStatusEvaluator.removeDeployedTopology(id, deployedTopology),
          )
        )(pollingExecutionContext)
      )
    } else {
      None
    }

  // alternatively there can be hardcoded kafka-clusters to monitor (at development time)
  // we assume here that additionalKafkaClustersEnabled and kafkaChangeLogStoreEnabled cannot be true at the same time
  // (hence the changelog can't mess with the additional ones and vica versa)
  if (additionalKafkaClustersEnabled) {
    additionalKafkaClusters.foreach {
      case (id, newKafkaCluster) =>
        val kafkaConnection = kafkaConnectionFrom(newKafkaCluster)
        // first make sure that the consumer group evaluator knows about this new kafka-cluster...
        consumerGroupStatusEvaluator.kafkaClusterCreated(id, kafkaConnection)
        // ... then we can tell the metrics-collectors too (so that by the time the consumer group evaluator receives lag information,
        // it knows about the kafka-cluster and can consume it to confirm the possible fake lags)
        kafkaClusterMetricsCollectors.createCollector(id, kafkaConnection)
    }
  }

  private val routes = handleExceptions(HttpExtensions.defaultExceptionHandler(logger)) {
    authDirective { user =>
      encodeResponseWith(Gzip) {
        ignoreTrailingSlash { KafkaClusterRoute.route(kafkaClusterService)(user) }
      }
    } ~ HealthRoute.route(kafkaClusterService) ~ HttpExtensions.testExceptionRoute
  }

  println(s"Startup completed, hosting on port $httpPort...")

  val finished = for {
    _ <- Http().bindAndHandle(
      // using the same time-out for requests as the akka ask time-out
      withRequestTimeout(timeout.duration)(corsHandler(routes)),
      "0.0.0.0", httpPort
    )
    waitOnFuture  <- Promise[Done].future
  } yield waitOnFuture

  sys.addShutdownHook {
    logger.info("ShutdownHook beginning...")
    kafkaClusterMetricsCollectors.close()
    kafkaChangeLogConsumer.foreach(_.stop())
    system.terminate()
    logger.info("ShutdownHook finished.")
  }

  Await.ready(finished, Duration.Inf)
  logger.info("application exiting.")
}
