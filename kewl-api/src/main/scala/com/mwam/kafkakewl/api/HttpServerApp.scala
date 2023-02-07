/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.HttpOrigin
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.codahale.metrics.jmx.JmxReporter
import com.mwam.kafkakewl.api.actors.CommandProcessorActor
import com.mwam.kafkakewl.api.routes._
import com.mwam.kafkakewl.common.PluginLoader
import com.mwam.kafkakewl.common.cache._
import com.mwam.kafkakewl.common.changelog.KafkaChangeLogStore
import com.mwam.kafkakewl.common.http.{CORSHandler, HttpExtensions}
import com.mwam.kafkakewl.common.metrics.{MetricsServiceClientCaching, MetricsServiceClientHttp, MetricsServiceImpl, MetricsServiceOps}
import com.mwam.kafkakewl.common.persistence.PersistentStoreFactory
import com.mwam.kafkakewl.common.validation.{PermissionStoreBuiltin, PermissionValidator}
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.extensions.{AuthPlugin, PermissionPlugin}
import com.mwam.kafkakewl.processor.state.{StateCommandProcessor, StateReadOnlyCommandProcessor}
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

  override val allAllowedOrigins = httpAllowedOrigins.map(HttpOrigin(_))

  implicit val system: ActorSystem = ActorSystem("kafkakewl-api")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  // the time-out needs to be longer than usual, because some operations can be really slow (e.g. topic deletion)
  implicit val timeout: Timeout = 10.minutes

  val jmpReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("kafkakewl-api").build
  jmpReporter.start()

  ApplicationMetrics.initialize()

  private val metricsService = metricsServiceUri.map {
    uri => new MetricsServiceImpl(
      new MetricsServiceClientCaching(new MetricsServiceClientHttp(uri, topicDefaults))
    ) with MetricsServiceOps
  }

  private val stateCommandProcessorConfig = StateCommandProcessor.Config(env, kafkaClusterCommandProcessorJaasConfig, metricsService, failFastIfStateStoreInvalid)
  private val kafkaClusterCommandProcessorConfig = CommandProcessorActor.KafkaClusterCommandProcessorConfig(env, kafkaClusterCommandProcessorJaasConfig, failFastIfDeploymentStateStoreInvalid)

  private val changeLogStoreOrNone = kafkaChangeLogStoreConnectionOrNone.map(new KafkaChangeLogStore(env, _, kafkaChangeLogStoreTopicConfig, timeout.duration))

  val (persistentStoreConfig, persistentStoreFactory) = PersistentStoreFactory.create(
    persistentStore,
    env,
    kafkaPersistentStoreConfigOrNone,
    sqlPersistentStoreConfigOrNone,
    changeLogStoreOrNone,
    timeout.duration
  )

  val authPlugins = PluginLoader.loadPlugins[AuthPlugin]()
  val permissionPlugins = PluginLoader.loadPlugins[PermissionPlugin]()

  logger.info(s"available processors:     ${Runtime.getRuntime.availableProcessors()}")
  logger.info(s"task-support parallelism: ${Seq.fill(50)(1).par.tasksupport.parallelismLevel}")
  logger.info(s"environment:              $env")
  logger.info(s"super-users:              $superUsers")
  logger.info(s"persistent-store:         $persistentStore")
  logger.info(s"persistent-store-config:  $persistentStoreConfig (${if (persistentStore == "sql") sqlDbConnectionInfoOrNone else "-"})")
  logger.info(s"kafka jaas config:        $kafkaClusterCommandProcessorJaasConfig")
  logger.info(s"metrics service:          ${metricsServiceUri.getOrElse("-")}")
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
  logger.info(s"available perm. plugins:  ${permissionPlugins.keys.mkString(", ")}")
  logger.info(s"chosen perm. plugin:      ${permissionPluginName.getOrElse("built-in")}")

  val authDirective = authPlugins
    .getOrElse(authPluginName, sys.error(s"couldn't find authentication plugin '$authPluginName'. Available plugins: ${authPlugins.keys.mkString(", ")}"))
    .createAuthenticationDirective(config)

  val permissionStoreBuiltinExtensionOrNone = permissionPluginName
    .map { permissionPluginName =>
      val permissionPlugin = permissionPlugins.getOrElse(permissionPluginName, sys.error(s"couldn't find permission plugin '$permissionPluginName'. Available plugins: ${permissionPlugins.keys.mkString(", ")}"))
      PermissionStoreBuiltin(permissionPlugin.createPermissionStore(config))
    }

  private val permissionValidator = new PermissionValidator(
    superUsers,
    permissionStoreBuiltinExtensionOrNone
      .map { permissionStoreBuiltinExtension =>
        (_ => permissionStoreBuiltinExtension): PermissionValidator.CreatePermissionStoreBuiltin
      }
      .getOrElse(PermissionStoreBuiltin.apply)
  )

  private val stateReadonlyCommandProcessor = new StateReadOnlyCommandProcessor(stateCommandProcessorConfig, permissionValidator, topicDefaults)
  private val stateStoresCache: AllStateEntitiesStateStoresCache = stateReadonlyCommandProcessor
  private val deployedTopologyStateStoresCache: DeployedTopologyStateStoresCache = stateReadonlyCommandProcessor

  private val commandProcessorActor: ActorRef =
    system.actorOf(
      Props(
        new CommandProcessorActor(
          persistentStoreFactory,
          kafkaClusterCommandProcessorConfig,
          permissionValidator,
          permissionStoreBuiltinExtensionOrNone,
          startWithWipe => new StateCommandProcessor(
            stateCommandProcessorConfig,
            permissionValidator,
            persistentStoreFactory,
            stateStoresCache,
            deployedTopologyStateStoresCache,
            startWithWipe,
            topicDefaults
          ),
          topicDefaults
        )
      ),
      "CommandProcessorActor"
    )


  private val routes = handleExceptions(HttpExtensions.defaultExceptionHandler(logger)) {
    authDirective { user =>
      encodeResponseWith(Gzip) {
        ignoreTrailingSlash {
          AdminRoute.route(commandProcessorActor)(user) ~
            TopologyRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) ~
            KafkaClusterRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) ~
            DeploymentRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) ~
            (if (permissionStoreBuiltinExtensionOrNone.isEmpty) PermissionRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) else reject) ~
            DeployedTopologyRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) ~
            DeployedTopologyMetricsRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user) ~
            ResolvedDeployedTopologyRoute.route(commandProcessorActor, stateReadonlyCommandProcessor)(user)
        }
      }
    } ~ HealthRoute.route(commandProcessorActor) ~ HttpExtensions.testExceptionRoute
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
    system.terminate()
    logger.info("ShutdownHook finished.")
  }

  Await.ready(finished, Duration.Inf)
  logger.info("application exiting.")
}
