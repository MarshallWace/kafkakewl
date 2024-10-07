/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

fun KafkaTopicPartitionInfos.diff(newTopicPartitionInfos: KafkaTopicPartitionInfos): KafkaTopicPartitionInfoChanges {
    val addedOrUpdated = newTopicPartitionInfos.filter {
        val topicPartition = it.key
        val newTopicPartitionInfo = it.value
        val existingTopicPartitionInfo = this[topicPartition]
        existingTopicPartitionInfo == null || !existingTopicPartitionInfo.equalsWithoutTimestamp(newTopicPartitionInfo)
    }
    val removed = this.keys - newTopicPartitionInfos.keys

    return KafkaTopicPartitionInfoChanges(addedOrUpdated, removed)
}

fun Map<String, KafkaSingleTopicPartitionInfos>.applyChanges(topicInfoChanges: KafkaTopicPartitionInfoChanges) : Map<String, KafkaSingleTopicPartitionInfos> {
    val newTopicInfos = topicInfoChanges.addedOrUpdated.entries
        .groupBy { it.key.topic }
        .mapValues { it.value.associate { it.key.partition to it.value } }

    val removedTopicPartitions = topicInfoChanges.removed
        .groupBy { it.topic }
        .mapValues { it.value.map { it.partition }.toSet() }

    return (this.keys + newTopicInfos.keys).associate { topic ->
        topic to (this.getOrElse(topic) { emptyKafkaSingleTopicPartitionInfos }
                + newTopicInfos.getOrElse(topic) { emptyKafkaSingleTopicPartitionInfos }
                - removedTopicPartitions.getOrElse(topic) { emptySet() })
    }
}
