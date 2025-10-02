/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import kotlinx.serialization.Serializable
import org.apache.kafka.common.TopicPartition
import kotlin.time.Instant

@Serializable
data class KafkaTopicPartition(val topic: String, val partition: Int) {
    constructor(topicPartition: TopicPartition) : this(topicPartition.topic(), topicPartition.partition())
}

@Serializable
data class KafkaTopicPartitionInfo(val beginningOffset: Long, val endOffset: Long, val lastOffsetTimestamp: Instant) {
    fun equalsWithoutTimestamp(other: KafkaTopicPartitionInfo): Boolean {
        return beginningOffset == other.beginningOffset && endOffset == other.endOffset
    }
}

// TODO this should be a SortedMap but kotlin does not have an immutable one - what should we do?
typealias KafkaSingleTopicPartitionInfos = Map<Int, KafkaTopicPartitionInfo>
val emptyKafkaSingleTopicPartitionInfos: KafkaSingleTopicPartitionInfos = emptyMap()

typealias KafkaTopicPartitionInfos = Map<KafkaTopicPartition, KafkaTopicPartitionInfo>
val emptyKafkaTopicPartitionInfos: KafkaTopicPartitionInfos = emptyMap()

@Serializable
data class KafkaTopicPartitionInfoChanges(
    val addedOrUpdated: Map<KafkaTopicPartition, KafkaTopicPartitionInfo>,
    val removed: Set<KafkaTopicPartition>
) {
    val isEmpty
        get() = addedOrUpdated.isEmpty() && removed.isEmpty()

    override fun toString(): String = "added-or-updated: ${addedOrUpdated.size}, removed: ${removed.size}"
}
