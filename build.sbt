import Dependencies._

ThisBuild / scalaVersion        := "2.12.17"
ThisBuild / version             := "0.2.0"
ThisBuild / scalacOptions       ++= Seq("-J-Xmx2G", "-J-Xss8M", "-Xexperimental", "-language:higherKinds", "-feature", "-unchecked", "-deprecation", "-Ywarn-unused-import",  "-Ypartial-unification")
ThisBuild / organization        := "com.mwam.kafkakewl"
ThisBuild / organizationName    := "Marshall Wace"
ThisBuild / startYear           := Some(2023)

// Have to do this for the root project and enable it for the sub-projects as well as setting the headerLicense for them.
disablePlugins(HeaderPlugin)

val license = Some(HeaderLicense.Custom(
  """SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
    |
    |SPDX-License-Identifier: Apache-2.0
    |""".stripMargin
))

val akkaVersion = "2.5.26"
val akkaHttpVersion = "10.1.11"
val circeVersion = "0.13.0"
val catsVersion = "1.4.0"
val metricsVersion = "4.0.5"
val kafkaVersion = "2.0.0" // Any version higher than 2.0.0 changes the KafkaConsumer.beginningOffsets()'s behaviour and makes it time-out if only a single topic-partition has no leader or some metadata-problem.
                           // This is really bad, because it can happen sometimes and then we won't have any lag monitoring (and low/high offset metrics) for the whole cluster.
                           // Unfortunately this old version fails on e.g. consuming zstd compressed message batches, which makes the consume-after-consumer-group feature (to detect fake-lags) less useful.
val curatorVersion = "5.5.0"

val dnsDeps = Seq(
  "dnsjava" % "dnsjava" % "2.1.8"
)

val configDeps = Seq(
  "com.typesafe" % "config" % "1.3.3"
)

val catsDeps = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-kernel" % catsVersion,
  "org.typelevel" %% "cats-macros" % catsVersion
)

val loggingDeps = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
)

val loggingImplDeps = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5",
  "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.1"
)

val jsonDeps = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-generic-extras"
).map(_ % circeVersion)

val akkaDeps = Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.22.0"
)

val akkaHttpDeps = Seq(
  "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-caching" % akkaHttpVersion
)

val akkaHttpSpNegoDeps = Seq(
  "com.tresata" %% "akka-http-spnego" % "0.4.0"
)

val miscDeps = Seq(
  "com.github.blemale" %% "scaffeine" % "3.1.0"
)

val sqlDeps = Seq(
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.0.0.jre8"
)

val kafkaDeps = Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion
)

val curatorDeps = Seq(
  "org.apache.curator" % "curator-framework" % curatorVersion
)

val testDeps = Seq(
  scalaTest % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test
)

val metricsDeps = Seq(
  "io.prometheus.jmx" % "jmx_prometheus_javaagent" % "0.11.0",
  "io.dropwizard.metrics" % "metrics-jmx" % "4.1.0",
  "nl.grons" %% "metrics4-scala" % metricsVersion,
  "nl.grons" %% "metrics4-akka_a24" % metricsVersion,
  "nl.grons" %% "metrics4-scala-hdr" % metricsVersion
)

def moduleIdFromString(extension: String): ModuleID = {
  val Array(organization, artifact, revision) = extension.split("\\|").map(_.trim)
  organization %% artifact % revision
}

def moduleIdsFromEnvVar(name: String): Seq[ModuleID] =
  Option(System.getenv(name)).toSeq
    .flatMap(_.split(Array('\n', ';'))).map(_.trim).filter(_.nonEmpty)
    .map(moduleIdFromString)

def moduleIdsFromFiles(files: java.io.File*): Seq[ModuleID] =
  files
    .filter(_.exists)
    .flatMap(IO.readLines(_).map(_.trim).filter(_.nonEmpty))
    .map(moduleIdFromString)

lazy val `kewl-utils` = (project in file("kewl-utils"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .settings(
    name := "kewl-utils",
    headerLicense := license,
    libraryDependencies ++= dnsDeps ++ configDeps ++ catsDeps
      ++ loggingDeps ++ loggingImplDeps ++ jsonDeps ++ testDeps ++ metricsDeps
  )

lazy val `kewl-kafka-utils` = (project in file("kewl-kafka-utils"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`)
  .settings(
    name := "kewl-kafka-utils",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ catsDeps ++ jsonDeps ++ testDeps ++ kafkaDeps ++ metricsDeps
  )

lazy val `kewl-extensions` = (project in file("kewl-extensions"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .settings(
    name := "kewl-extensions",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ akkaHttpDeps
  )

lazy val `kewl-extensions-builtin` = (project in file("kewl-extensions-builtin"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-extensions`)
  .settings(
    name := "kewl-extensions-builtin",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ loggingDeps ++ akkaHttpSpNegoDeps
  )

lazy val `kewl-domain` = (project in file("kewl-domain"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`)
  .settings(
    name := "kewl-domain",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ catsDeps ++ jsonDeps ++ testDeps ++ kafkaDeps
  )

lazy val `kewl-common` = (project in file("kewl-common"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-extensions`, `kewl-domain` % "compile->compile;test->test")
  .settings(
    name := "kewl-common",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ akkaDeps ++ catsDeps ++ akkaHttpDeps ++ jsonDeps ++ testDeps
      ++ kafkaDeps ++ miscDeps ++ sqlDeps ++ metricsDeps
  )

lazy val `kewl-state-processor` = (project in file("kewl-state-processor"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-domain` % "compile->compile;test->test", `kewl-common` % "compile->compile;test->test")
  .settings(
    name := "kewl-state-processor",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ akkaDeps ++ akkaHttpDeps ++ jsonDeps
      ++ kafkaDeps ++ testDeps ++ metricsDeps
  )

lazy val `kewl-kafkacluster-processor` = (project in file("kewl-kafkacluster-processor"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-domain` % "compile->compile;test->test", `kewl-common` % "compile->compile;test->test")
  .settings(
    name := "kewl-kafkacluster-processor",
    headerLicense := license,
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ akkaDeps ++ akkaHttpDeps
      ++ jsonDeps ++ kafkaDeps ++ curatorDeps ++ testDeps ++ metricsDeps
  )

lazy val `kewl-api` = (project in file("kewl-api"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-extensions`, `kewl-extensions-builtin`, `kewl-domain`, `kewl-common`, `kewl-state-processor`, `kewl-kafkacluster-processor`)
  .settings(
    name := "kewl-api",
    headerLicense := license,
    mainClass in (Compile, stage) := Some("com.mwam.kafkakewl.api.HttpServerApp"),
    scriptClasspath in bashScriptDefines ~= (cp => "extensions/*.jar" +: cp),
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ loggingImplDeps ++ akkaDeps ++ akkaHttpDeps
      ++ jsonDeps ++ kafkaDeps ++ miscDeps ++ testDeps ++ metricsDeps,

    libraryDependencies ++= moduleIdsFromEnvVar("KAFKAKEWL_API_EXTENSIONS"),
    libraryDependencies ++= moduleIdsFromFiles(file(".kewl-api-extensions").getAbsoluteFile, Path.userHome / ".kewl-api-extensions")
  )

lazy val `kewl-stateadaptor` = (project in file("kewl-stateadaptor"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-extensions`, `kewl-extensions-builtin`, `kewl-domain`, `kewl-common`)
  .settings(
    name := "kewl-stateadaptor",
    headerLicense := license,
    mainClass in (Compile, stage) := Some("com.mwam.kafkakewl.stateadaptor.MainApp"),
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ loggingImplDeps ++ akkaDeps ++ akkaHttpDeps
      ++ jsonDeps ++ kafkaDeps ++ miscDeps ++ testDeps ++ metricsDeps
  )

lazy val `kewl-api-metrics` = (project in file("kewl-api-metrics"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-extensions`, `kewl-extensions-builtin`, `kewl-domain`, `kewl-common`, `kewl-state-processor`, `kewl-kafkacluster-processor`)
  .settings(
    name := "kewl-api-metrics",
    headerLicense := license,
    mainClass in (Compile, stage) := Some("com.mwam.kafkakewl.api.metrics.HttpServerApp"),
    scriptClasspath in bashScriptDefines ~= (cp => "extensions/*.jar" +: cp),
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ loggingImplDeps ++ akkaDeps ++ akkaHttpDeps
      ++ jsonDeps ++ kafkaDeps ++ miscDeps ++ testDeps ++ metricsDeps,

    libraryDependencies ++= moduleIdsFromEnvVar("KAFKAKEWL_API_METRICS_EXTENSIONS"),
    libraryDependencies ++= moduleIdsFromFiles(file(".kewl-api-metrics-extensions").getAbsoluteFile, Path.userHome / ".kewl-api-metrics-extensions")
  )

lazy val `kewl-api-migrate` = (project in file("kewl-migrate"))
  .enablePlugins(JavaAppPackaging, AutomateHeaderPlugin)
  .dependsOn(`kewl-utils`, `kewl-kafka-utils`, `kewl-extensions`, `kewl-extensions-builtin`, `kewl-domain`, `kewl-common`, `kewl-state-processor`, `kewl-kafkacluster-processor`)
  .settings(
    name := "kewl-migrate",
    headerLicense := license,
    mainClass in (Compile, stage) := Some("com.mwam.kafkakewl.migrate.HttpServerApp"),
    scriptClasspath in bashScriptDefines ~= (cp => "extensions/*.jar" +: cp),
    libraryDependencies ++= configDeps ++ catsDeps ++ loggingDeps ++ loggingImplDeps ++ akkaDeps ++ akkaHttpDeps
      ++ jsonDeps ++ kafkaDeps ++ miscDeps ++ testDeps ++ metricsDeps,
    libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.2",
    libraryDependencies += "com.lihaoyi" %% "requests" % "0.9.0",

    libraryDependencies ++= moduleIdsFromEnvVar("KAFKAKEWL_MIGRATE_EXTENSIONS"),
    libraryDependencies ++= moduleIdsFromFiles(file(".kewl-migrate-extensions").getAbsoluteFile, Path.userHome / ".kewl-api-migrate-extensions")
  )
