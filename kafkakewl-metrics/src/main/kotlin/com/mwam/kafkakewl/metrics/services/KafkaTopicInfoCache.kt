/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.KafkaSingleTopicPartitionInfos
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoChanges
import com.mwam.kafkakewl.metrics.domain.applyChanges
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicReference

interface KafkaTopicInfoCache : AutoCloseable {
    fun getTopics(): List<String>
    fun getTopicPartitionInfos(topic: String): KafkaSingleTopicPartitionInfos?
}

private val logger = KotlinLogging.logger {}

class KafkaTopicInfoCacheImpl(
    source: KafkaTopicInfoSource
) : KafkaTopicInfoCache {
    private val scope = CoroutineScope(Dispatchers.Default)
    private val topicInfoChangesCollectJob: Job?
    private val topicPartitionInfosRef: AtomicReference<Map<String, KafkaSingleTopicPartitionInfos>> = AtomicReference(emptyMap())

    init {
        topicInfoChangesCollectJob = scope.launch { source.topicInfoChanges().collect { onTopicInfoChanges(it)} }
    }

    override fun getTopics(): List<String> = topicPartitionInfosRef.get().keys.sorted()
    override fun getTopicPartitionInfos(topic: String): KafkaSingleTopicPartitionInfos? = topicPartitionInfosRef.get()[topic]

    override fun close() {
        logger.info { "Closing..." }
        runBlocking {
            scope.cancel()
            topicInfoChangesCollectJob?.join()
        }
        logger.info { "Finished closing" }
    }

    private fun onTopicInfoChanges(changes: KafkaTopicPartitionInfoChanges): Unit {
        topicPartitionInfosRef.set(topicPartitionInfosRef.get().applyChanges(changes))
    }
}
