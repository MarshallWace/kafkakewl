/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/** A surrogate type for Application so that the type polymorphic field can be flattened - the resulting json is human friendlier */
@Serializable
@SerialName("Application")
data class ApplicationSurrogate(
    val id: ApplicationLocalId,
    val user: UserId,
    val consumerGroup: String? = null,
    val transactionalId: String? = null,
    val kafkaStreamsAppId: String? = null,
    val connector: String? = null,
    val connectReplicator: String? = null,
    val host: Host? = null,
    val description: String? = null,
    val canConsume: List<Matcher> = emptyList(),
    val canProduce: List<Matcher> = emptyList(),
    val consumerLagWindowSeconds: Int? = null,
    val tags: List<String> = emptyList(),
    val labels: Map<String, String> = emptyMap()
)

object ApplicationSerializer : KSerializer<Application> {
    override val descriptor: SerialDescriptor = ApplicationSurrogate.serializer().descriptor

    override fun serialize(encoder: Encoder, value: Application) {
        val surrogate = ApplicationSurrogate(
            value.id,
            value.user,
            when (value.type) { is ApplicationTypes.Simple -> value.type.consumerGroup else -> null },
            when (value.type) { is ApplicationTypes.Simple -> value.type.transactionalId else -> null },
            when (value.type) { is ApplicationTypes.KafkaStreams -> value.type.kafkaStreamsAppId else -> null },
            when (value.type) { is ApplicationTypes.Connector -> value.type.connector else -> null },
            when (value.type) { is ApplicationTypes.ConnectReplicator -> value.type.connectReplicator else -> null },
            value.host,
            value.description,
            value.canConsume,
            value.canProduce,
            value.consumerLagWindowSeconds,
            value.tags,
            value.labels
        )
        encoder.encodeSerializableValue(ApplicationSurrogate.serializer(), surrogate)
    }

    override fun deserialize(decoder: Decoder): Application {
        val surrogate = decoder.decodeSerializableValue(ApplicationSurrogate.serializer())

        val isKafkaStreams = surrogate.kafkaStreamsAppId != null
        val isConnector = surrogate.connector != null
        val isConnectReplicator = surrogate.connectReplicator != null
        val isSimple = !isKafkaStreams && !isConnector && !isConnectReplicator
        val hasSimpleProperties = surrogate.consumerGroup != null || surrogate.transactionalId != null

        require(isSimple || !hasSimpleProperties) { "a non-simple application must not have consumerGroup or transactionalId specified" }
        require(isSimple ||
                isKafkaStreams && !isConnector && !isConnectReplicator ||
                !isKafkaStreams && isConnector && !isConnectReplicator ||
                !isKafkaStreams && !isConnector && isConnectReplicator
        ) { "only one of kafkaStreamsAppId or connector or connectReplicator can be specified" }

        val applicationType = if (isSimple) ApplicationTypes.Simple(surrogate.consumerGroup, surrogate.transactionalId)
                              else if (isKafkaStreams) ApplicationTypes.KafkaStreams(surrogate.kafkaStreamsAppId)
                              else if (isConnector) ApplicationTypes.Connector(surrogate.connector)
                              else if (isConnectReplicator) ApplicationTypes.ConnectReplicator(surrogate.connectReplicator)
                              else throw IllegalStateException("unreachable code")

        return Application(
            surrogate.id,
            surrogate.user,
            applicationType,
            surrogate.host,
            surrogate.description,
            surrogate.canConsume,
            surrogate.canProduce,
            surrogate.consumerLagWindowSeconds,
            surrogate.tags,
            surrogate.labels
        )
    }
}
