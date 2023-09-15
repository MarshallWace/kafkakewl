val orgName = "Marshall Wace"
val year = 2023

ThisBuild / scalaVersion        := "3.3.0"
ThisBuild / version             := "0.1.0-SNAPSHOT"
ThisBuild / organization        := "com.mwam.kafkakewl"
ThisBuild / organizationName    := orgName
ThisBuild / startYear           := Some(year)

ThisBuild / scalacOptions       ++= Seq("-Xmax-inlines", "64", "-Wunused:imports", "-Wunused:params", "-deprecation", "-feature")

// Have to do this for the root project and enable it for the sub-projects as well as setting the headerLicense for them.
disablePlugins(HeaderPlugin)

val license = Some(HeaderLicense.Custom(
  s"""SPDX-FileCopyrightText: $year $orgName <opensource@mwam.com>
     |
     |SPDX-License-Identifier: Apache-2.0
     |""".stripMargin
))

val logbackVersion = "1.4.11"
val logbackContribJsonVersion = "0.1.5"
val tapirVersion = "1.7.3"
val zioVersion = "2.0.16"
val zioMetricsConnectorsVersion = "2.1.0"
val tapirZioJsonVersion = "3.9.0"
val zioJsonVersion = "0.6.1"
val zioConfigVersion = "3.0.7"
val zioLoggingVersion = "2.1.14"
val zioKafkaVersion = "2.4.2"
val zioTelemetryVersion = "3.0.0-RC17"
val openTelemetryVersion = "1.29.0"
val openTelemetryGrpcVersion = "1.47.0"
val kafkaClientVersion = "3.5.1"

val tapir = Seq(
  "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-json-zio" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-zio-metrics" % tapirVersion,
)

val tapirCore = Seq(
  "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion
)

val zio = Seq(
  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-metrics-connectors" % zioMetricsConnectorsVersion,
  "dev.zio" %% "zio-metrics-connectors-prometheus" % zioMetricsConnectorsVersion,
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  "dev.zio" %% "zio-json" % zioJsonVersion,
  "dev.zio" %% "zio-logging" % zioLoggingVersion,
  "dev.zio" %% "zio-logging-slf4j" % zioLoggingVersion,
  "dev.zio" %% "zio-kafka" % zioKafkaVersion,
  "com.softwaremill.sttp.client3" %% "zio-json" % tapirZioJsonVersion % Test
)

val config = Seq(
  "dev.zio" %% "zio-config" % zioConfigVersion,
  "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,
  "dev.zio" %% "zio-config-magnolia" % zioConfigVersion
)

val kafkaClient = Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaClientVersion
)

val logging = Seq(
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackContribJsonVersion
)

val telemetry = Seq(
  "dev.zio" %% "zio-opentelemetry" % zioTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-api" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-sdk" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-exporter-otlp" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-extension-trace-propagators" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-semconv" % s"$openTelemetryVersion-alpha",
  "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % openTelemetryVersion,
  "io.grpc" % "grpc-netty-shaded" % openTelemetryGrpcVersion,
)

val tests = Seq(
  "com.softwaremill.sttp.tapir" %% "tapir-sttp-stub-server" % tapirVersion % Test
)

lazy val utils = project
  .enablePlugins(AutomateHeaderPlugin)
  .in(file("kafkakewl-utils"))
  .settings(
    name := "kafkakewl-utils",
    headerLicense := license,
    libraryDependencies ++= logging ++ zio ++ config
  )

lazy val domain = project
  .enablePlugins(AutomateHeaderPlugin)
  .in(file("kafkakewl-domain"))
  .dependsOn(utils)
  .settings(
    name := "kafkakewl-domain",
    headerLicense := license,
    libraryDependencies ++= tapirCore ++ zio ++ tests
  )

lazy val common = project
  .enablePlugins(AutomateHeaderPlugin)
  .in(file("kafkakewl-common"))
  .dependsOn(utils, domain)
  .settings(
    name := "kafkakewl-common",
    headerLicense := license,
    // TODO we need to depend on tapirCore ++ zioHttp only, but tapir is simpler
    libraryDependencies ++= tapir ++ zio ++ tests ++ telemetry
  )

lazy val deploy = project
  .enablePlugins(AutomateHeaderPlugin)
  .in(file("kafkakewl-deploy"))
  .dependsOn(utils, domain, common)
  .settings(
    name := "kafkakewl-deploy",
    headerLicense := license,
    libraryDependencies ++= tapir ++ tapirCore ++ config ++ zio ++ tests ++ logging,
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val metrics = project
  .enablePlugins(AutomateHeaderPlugin)
  .in(file("kafkakewl-metrics"))
  .dependsOn(utils, domain, common)
  .settings(
    name := "kafkakewl-metrics",
    headerLicense := license,
    libraryDependencies ++= tapir ++ tapirCore ++ config ++ zio ++ tests ++ logging,
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
