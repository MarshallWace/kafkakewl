/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

final case class DeploymentEnvironmentId(id: String) extends IdLike
object DeploymentEnvironment {
  object Variable {
    final case class Value(values: Seq[String]) extends AnyVal
    object Value {
      def apply(value: String) = new Value(Seq(value))
    }
  }

  type Variables = Map[String, Variable.Value]
  object Variables {
    def apply(v: (String, Variable.Value)*): Variables = v.toMap
    def empty = Map.empty[String, Variable.Value]
  }
}
object DeploymentEnvironments {
  type Variables = Map[DeploymentEnvironmentId, DeploymentEnvironment.Variables]
  object Variables {
    def apply(v: (DeploymentEnvironmentId, DeploymentEnvironment.Variables)*) = Map(v: _*)
    def empty: Variables =  Map.empty[DeploymentEnvironmentId, DeploymentEnvironment.Variables]

    /**
      * The built-in deployment environment variable names
      */
    object Builtin {
      val developersAccess = "developers-access"
      val env = "env"
      val shortEnv = "short-env"
      val deployWithAuthorizationCode = "deploy-with-authorization-code"
    }

    /**
      * The default values for some of the deployment variables
      */
    val defaults = DeploymentEnvironments.Variables(
      DeploymentEnvironments.default -> DeploymentEnvironment.Variables(
        // Builtin.developersAccess has a "very" default, because it's used from the code as the default expression for Topology.developersAccess
        // (making sure that the topology can be deployed to any kafka-cluster, even if it doesn't specify it explicitly)
        // Assuming that every kafka-cluster is part of the "default" environment, which is true, see KafkaClusterValidator
        Builtin.developersAccess -> DeploymentEnvironment.Variable.Value("TopicReadOnly"),
        // Builtin.deployWithAuthorizationCode has a "very" default too
        Builtin.deployWithAuthorizationCode -> DeploymentEnvironment.Variable.Value("false")
      ),
      DeploymentEnvironments.test -> DeploymentEnvironment.Variables(
        // these are here only to make life easier: if a kafka-cluster is part of "test", it gets these automatically (they can be overriden in the cluster or later in the topology)
        Builtin.env -> DeploymentEnvironment.Variable.Value("test"),
        Builtin.shortEnv -> DeploymentEnvironment.Variable.Value("t"),
        Builtin.developersAccess -> DeploymentEnvironment.Variable.Value("Full"),
        Builtin.deployWithAuthorizationCode -> DeploymentEnvironment.Variable.Value("false")
      ),
      DeploymentEnvironments.staging -> DeploymentEnvironment.Variables(
        // these are here only to make life easier: if a kafka-cluster is part of "staging", it gets these automatically (they can be overriden in the cluster or later in the topology)
        Builtin.env -> DeploymentEnvironment.Variable.Value("staging"),
        Builtin.shortEnv -> DeploymentEnvironment.Variable.Value("s"),
        Builtin.developersAccess -> DeploymentEnvironment.Variable.Value("TopicReadOnly"),
        Builtin.deployWithAuthorizationCode -> DeploymentEnvironment.Variable.Value("false")
      ),
      DeploymentEnvironments.prod -> DeploymentEnvironment.Variables(
        // same as above but with prod
        Builtin.env -> DeploymentEnvironment.Variable.Value("prod"),
        Builtin.shortEnv -> DeploymentEnvironment.Variable.Value("p"),
        Builtin.developersAccess -> DeploymentEnvironment.Variable.Value("TopicReadOnly"),
        Builtin.deployWithAuthorizationCode -> DeploymentEnvironment.Variable.Value("true")
      )
    )
  }

  /**
    * Same as Variables but it's just a sequence to keep the order. It's used in the KafkaCluster because there the order of
    * environments is essential.
    *
    * Not using ListMap or LinkedHashMap because their equality is still based on unordered maps (ie two instances with different order are still equal).
    */
  type OrderedVariables = Seq[(DeploymentEnvironmentId, DeploymentEnvironment.Variables)]
  object OrderedVariables {
    def apply(v: (DeploymentEnvironmentId, DeploymentEnvironment.Variables)*) = Seq(v: _*)
    def empty: OrderedVariables =  Seq.empty[(DeploymentEnvironmentId, DeploymentEnvironment.Variables)]
  }

  val default = DeploymentEnvironmentId("default")
  val test = DeploymentEnvironmentId("test")
  val staging = DeploymentEnvironmentId("staging")
  val prod = DeploymentEnvironmentId("prod")
}
