/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

import com.mwam.kafkakewl.common.config.KafkaClientConfig
import com.mwam.kafkakewl.common.config.KafkaPersistentStoreConfig
import com.mwam.kafkakewl.common.config.toKafkaClientConfig
import com.mwam.kafkakewl.domain.TopologyDeployment
import com.mwam.kafkakewl.domain.TopologyDeployments
import com.mwam.kafkakewl.domain.TopologyId
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.DescribeConfigsOptions
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Serializable
data class BatchMessageEnvelope<Payload>(val batchSize: Int, val indexInBatch: Int, val payload: Payload)

private val logger = KotlinLogging.logger {}

class KafkaPersistentStore(
    private val kafkaClientConfig: KafkaClientConfig,
    private val kafkaPersistentStoreConfig: KafkaPersistentStoreConfig
) : PersistentStore, AutoCloseable {
    companion object {
        private const val TOPIC_NUM_PARTITIONS = 1
    }

    private val producer: KafkaProducer<String, String>

    init {
        val adminConfig = kafkaClientConfig.toKafkaClientConfig()
        KafkaAdminClient.create(adminConfig).use { adminClient ->
            if (adminClient.listTopics().names().get().contains(topic)) {
                // The topic already exists
                if (kafkaPersistentStoreConfig.topic.reCreate) {
                    reCreateTopic(adminClient)
                } else {
                    checkExistingTopic(adminClient)
                }
            } else {
                // The topic does not exist yet
                createTopic(adminClient)
            }
        }

        val producerConfig = adminConfig + mapOf(ProducerConfig.TRANSACTIONAL_ID_CONFIG to kafkaPersistentStoreConfig.transactionalId)
        producer = KafkaProducer<String, String>(producerConfig, StringSerializer(), StringSerializer())
        producer.initTransactions()
    }

    override fun loadLatest(): Result<TopologyDeployments> {
        return runCatching {
            val (topologyDeployments, _) = loadMessagesUntilEnd<TopologyDeployments, String, String>(
                kafkaClientConfig,
                topic,
                StringDeserializer(),
                StringDeserializer(),
                { emptyMap() },
                // TODO json deserialization errors should include offset or the json itself
                { msgs, cr -> msgs + (TopologyId(cr.key()) to Json.decodeFromString<BatchMessageEnvelope<TopologyDeployment>>(cr.value()).payload) },
            )

            topologyDeployments
        }
    }

    override fun save(topologyDeployments: TopologyDeployments): Result<Unit> {
        return runCatching {
            try {
                producer.beginTransaction()
                val batchSize = topologyDeployments.size
                topologyDeployments.entries
                    .forEachIndexed { indexInBatch, entry ->
                        val key = entry.key.value
                        val value = Json.encodeToString(BatchMessageEnvelope(batchSize, indexInBatch, entry.value))
                        val record = ProducerRecord(topic, key, value)
                        // No callback, in case of failure, commitTransaction will throw, and we'll get the exception anyway
                        producer.send(record)
                    }
                producer.commitTransaction()
            } catch (e: Exception) {
                producer.abortTransaction()
                throw e
            }
        }
    }

    override fun stream(compactHistory: Boolean): Flow<TopologyDeployments> {
        TODO("not implemented")
    }

    override fun close() {
        producer.close()
    }

    private val topic: String
        get() = kafkaPersistentStoreConfig.topic.name

    private val replicationFactor: Short
        get() = kafkaPersistentStoreConfig.topic.replicationFactor ?: -1

    private val topicConfig: Map<String, String>
        get() = kafkaPersistentStoreConfig.topic.config + (TopicConfig.RETENTION_MS_CONFIG to "-1")

    private fun createTopic(adminClient: AdminClient) {
        val newTopic = NewTopic(topic, TOPIC_NUM_PARTITIONS, replicationFactor).configs(topicConfig)
        logger.info { "Creating topic $topic with partitions = $TOPIC_NUM_PARTITIONS, replicationFactor = $replicationFactor, config = $topicConfig ..." }
        adminClient.createTopics(listOf(newTopic)).all().get()
        logger.info { "Finished creating topic $topic" }
    }

    private fun checkExistingTopic(adminClient: AdminClient) {
        val topicDescription = adminClient.describeTopics(listOf(topic)).allTopicNames().get()[topic]!!
        val actualNumPartitions = topicDescription.partitions().map { it.partition() }.max() + 1
        if (actualNumPartitions != TOPIC_NUM_PARTITIONS) {
            throw RuntimeException("Topic $topic has $actualNumPartitions partitions but expected $TOPIC_NUM_PARTITIONS. Use a different topic for persistence or delete the $topic topic and let kafkakewl re-create it with the right number of partitions")
        }
        val actualNumReplicas = topicDescription.partitions().first().replicas().size.toShort()
        if (replicationFactor != (-1).toShort() && actualNumReplicas != replicationFactor) {
            throw RuntimeException("Topic $topic has $actualNumReplicas replicas but expected $replicationFactor. Use a different topic for persistence or delete the $topic topic and let kafkakewl re-create it with the right number of partitions")
        }
        val topicConfigResource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        val actualConfig = adminClient.describeConfigs(listOf(topicConfigResource), DescribeConfigsOptions()).all().get()[topicConfigResource]!!
            .entries()
            .filter { it.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG }
            .associate { it.name()!! to it.value()!! }
        if (actualConfig != topicConfig) {
            throw RuntimeException("Topic $topic has $actualConfig config but expected $topicConfig. Use a different topic for persistence or delete the $topic topic and let kafkakewl re-create it with the right number of partitions")
        }
    }

    private fun reCreateTopic(adminClient: AdminClient) {
        deleteTopic(adminClient)
        createTopic(adminClient)
    }

    private fun deleteTopic(adminClient: AdminClient) {
        logger.info { "Deleting topic $topic ..." }
        adminClient.deleteTopics(listOf(topic)).all().get()
        val delay = 5.seconds.toJavaDuration()
        logger.info { "Finished deleting $topic, waiting $delay so that it's properly deleted" }
        Thread.sleep(delay)
    }

    private fun <Messages, K, V> loadMessagesUntilEnd(
        kafkaClientConfig: KafkaClientConfig,
        topic: String,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        initialMessages: () -> Messages,
        combine: (Messages, ConsumerRecord<K, V>) -> Messages,
        pollTimeout: Duration = 100.milliseconds,
    ): Pair<Messages, Map<TopicPartition, Long>> {
        val consumerConfig = kafkaClientConfig.toKafkaClientConfig() + mapOf(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )
        val pollTimeoutJava = pollTimeout.toJavaDuration()
        KafkaConsumer<K, V>(consumerConfig, keyDeserializer, valueDeserializer).use { consumer ->
            val topicPartitions = consumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
            val endOffsets = consumer.endOffsets(topicPartitions).map { it.key!! to it.value!! }.toMap()

            var positions = emptyMap<TopicPartition, Long>()
            var messages = initialMessages()
            consumer.assign(topicPartitions)
            do {
                val consumerRecords = consumer.poll(pollTimeoutJava)
                messages = consumerRecords.fold(messages, combine)
                positions = endOffsets.mapValues { consumer.position(it.key) }
                val hasNotReachedTheEnd = endOffsets.any { it.value > positions.getValue(it.key) }
            } while (hasNotReachedTheEnd)
            return messages to positions
        }
    }
}
