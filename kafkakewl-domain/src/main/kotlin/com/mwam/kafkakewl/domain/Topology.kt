/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration

/** The fully qualified topic id (currently the same as the topic name). */
@JvmInline
@Serializable
value class TopicId(override val value: String) : StringValue

@JvmInline
@Serializable
value class TopicConfigDefaultsId(override val value: String) : StringValue

@JvmInline
@Serializable
value class TopicConfigKey(override val value: String) : StringValue

@JvmInline
@Serializable
value class TopicConfigValue(override val value: String) : StringValue

@Serializable
data class Topic(
    val name: String,
    val partitions: Int = 1,
    val replicationFactor: Short? = null,
    val configDefaults: TopicConfigDefaultsId? = null,
    val config: Map<TopicConfigKey, TopicConfigValue> = emptyMap(),
    val unManaged: Boolean = false,
    val description: String? = null,
    val canBeConsumed: List<Matcher> = emptyList(),
    val canBeProduced: List<Matcher> = emptyList(),
    val tags: List<String> = emptyList(),
    val labels: Map<String, String> = emptyMap()
) {

    /** The topic's fully qualified id is the same as the name. */
    val id: TopicId get() = TopicId(name)
}

/** The local application id in the current topology's namespace. */
@JvmInline
@Serializable
value class ApplicationLocalId(override val value: String) : StringValue

@JvmInline
@Serializable
value class UserId(override val value: String) : StringValue

@JvmInline
@Serializable
value class Host(override val value: String) : StringValue

/** This does not need to be Serializable because the Application is serialized via the ApplicationSurrogate */
sealed interface ApplicationType
object ApplicationTypes {
    data class Simple(val consumerGroup: String? = null, val transactionalId: String? = null) : ApplicationType
    data class KafkaStreams(val kafkaStreamsAppId: String) : ApplicationType
    data class Connector(val connector: String) : ApplicationType
    data class ConnectReplicator(val connectReplicator: String) : ApplicationType // TODO remove legacy
}

@Serializable(with = ApplicationSerializer::class)
data class Application(
    val id: ApplicationLocalId,
    val user: UserId,
    val type: ApplicationType,
    val host: Host? = null,
    val description: String? = null,
    val canConsume: List<Matcher> = emptyList(),
    val canProduce: List<Matcher> = emptyList(),
    val consumerLagWindowSeconds: Int? = null, // TODO is this the best way to configure the consumer status logic? Maybe a separate sub-object in case we want more fields?
    val tags: List<String> = emptyList(),
    val labels: Map<String, String> = emptyMap()
)

/** The local topic alias id in the current topology's namespace (fully qualified isn't really needed anyway because aliases aren't currently exposed
 * to other topologies).
 */
@JvmInline
@Serializable
value class TopicAliasLocalId(override val value: String) : StringValue

@Serializable
data class TopicAlias(
    val id: TopicAliasLocalId,
    // TODO perhaps support other ways, e.g. list of topic ids, namespace? Although all these are expressible with regex easily
    val regex: String
)

/** The local application alias id in the current topology's namespace (fully qualified isn't really needed anyway because aliases aren't currently
 * exposed to other topologies).
 */
@JvmInline
@Serializable
value class ApplicationAliasLocalId(override val value: String) : StringValue

@Serializable
data class ApplicationAlias(
    val id: ApplicationAliasLocalId,
    // TODO perhaps support other ways, e.g. list of topic ids, namespace? Although all these are expressible with regex easily
    val regex: String
)

@Serializable
data class Aliases(
    val topics: List<TopicAlias> = emptyList(),
    val applications: List<ApplicationAlias> = emptyList()
)

/** ApplicationFlexId can be an application alias or application id, local or fully qualified.
 */
@JvmInline
@Serializable
value class ApplicationFlexId(override val value: String) : StringValue

/** TopicFlexId can be a local or fully qualified topic alias or fully qualified topic id.
 */
@JvmInline
@Serializable
value class TopicFlexId(override val value: String) : StringValue

@Serializable
data class ProducedTopic(val topic: TopicFlexId)

@Serializable
data class ConsumedTopic(val topic: TopicFlexId)

@Serializable
data class Relationship(
    val application: ApplicationFlexId,
    val produce: List<ProducedTopic> = emptyList(),
    val consume: List<ConsumedTopic> = emptyList()
)

@JvmInline
@Serializable
value class TopologyId(override val value: String) : StringValue

@JvmInline
@Serializable
value class Namespace(override val value: String) : StringValue

@JvmInline
@Serializable
value class Developer(override val value: String) : StringValue

@Serializable
enum class DevelopersAccess {
    Full,
    TopicReadOnly
}

@Serializable
data class Topology(
    val id: TopologyId,
    val namespace: Namespace,
    val description: String? = null,
    val developers: List<Developer> = emptyList(),
    val developersAccess: DevelopersAccess = DevelopersAccess.TopicReadOnly,
    val topics: List<Topic> = emptyList(),
    val applications: List<Application> = emptyList(),
    val aliases: Aliases = Aliases(),
    val relationships: List<Relationship> = emptyList(),
    val tags: List<String> = emptyList(),
    val labels: Map<String, String> = emptyMap()
)

typealias Topologies = Map<TopologyId, Topology>
