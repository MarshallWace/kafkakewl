/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.kafka

import com.mwam.kafkakewl.domain.Topologies
import zio.*
import zio.kafka.admin.AdminClient

// TODO Should this be a service? Don't think so, it's stateless, no lifecycle, it's really just a pure function. We'll see.
object KafkaClusterItems {

  /** Creates the [[KafkaClusterItem]]s for the given topologies.
    *
    * @param topologies
    *   the topologies
    * @param isKafkaClusterSecurityEnabled
    *   a boolean flag indicating whether the cluster supports security (acls) or not
    */
  def forTopologies(
      topologies: Topologies,
      isKafkaClusterSecurityEnabled: Boolean
      // TODO we'll probably need some topic defaults description for e.g. default external consumer/producer namespaces, etc... to find out all relationships for topologies so that developers can get those acls
      // TODO we'll probably need a function that resolves the topics' replica placement id and populates it into the topic config - alternatively we can do that outside
  ): Seq[KafkaClusterItem] = {
    ???
  }

  /** Creates the [[KafkaClusterItem]]s that exist in the given kafka cluster.
    *
    * @param adminClient
    *   the admin client to use
    */
  def forKafkaCluster(adminClient: AdminClient): Task[Seq[KafkaClusterItem]] = {
    ???
  }
}
