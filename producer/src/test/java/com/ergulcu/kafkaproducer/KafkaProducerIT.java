// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer;

import com.ergulcu.kafka.contract.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Publishes through the REST endpoint into a real Kafka broker and reads the record back. */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class KafkaProducerIT {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.4.0");

    @Autowired
    private MockMvcTester mvc;

    @Test
    void createsTheTopicsAtStartup() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            assertThat(admin.listTopics().names().get())
                    .contains(Topics.MESSAGES, Topics.deadLetterOf(Topics.MESSAGES));
            assertThat(admin.describeTopics(List.of(Topics.MESSAGES)).allTopicNames().get()
                    .get(Topics.MESSAGES).partitions()).hasSize(3);
        }
    }

    @Test
    void aPostedMessageArrivesAsJsonKeyedByItsId() {
        assertThat(mvc.post().uri("/message").contentType(MediaType.TEXT_PLAIN).content("Kafka'ya merhaba"))
                .hasStatus(HttpStatus.ACCEPTED);

        List<ConsumerRecord<String, String>> records = readAll(Topics.MESSAGES, 1);

        assertThat(records).anySatisfy(record -> {
            assertThat(record.value()).contains("\"text\":\"Kafka'ya merhaba\"").contains("\"createdAt\"");
            assertThat(record.value()).contains("\"id\":\"" + record.key() + "\"");
            assertThat(record.headers().lastHeader("__TypeId__")).isNull();
        });
    }

    private static List<ConsumerRecord<String, String>> readAll(String topic, int atLeast) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(20);
            while (records.size() < atLeast && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        return records;
    }
}
