// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer;

import com.ergulcu.kafka.contract.Topics;
import com.ergulcu.kafkaconsumer.service.ReceivedMessages;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Sends records into a real broker and checks what the listener and the dead letter topic got. */
@SpringBootTest(properties = "app.kafka.retry-backoff=100ms")
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class KafkaConsumerIT {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.4.0");

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @Autowired
    private ReceivedMessages received;

    @Autowired
    private MockMvcTester mvc;

    @Test
    void validMessagesAreReceived() throws Exception {
        template.send(Topics.MESSAGES, "a1", """
                {"id":"a1","text":"Merhaba Kafka","createdAt":"2026-01-01T10:00:00Z"}""").get();

        awaitUntil(() -> received.messages().stream().anyMatch(m -> m.event().id().equals("a1")));

        assertThat(mvc.get().uri("/messages")).hasStatusOk()
                .bodyJson().extractingPath("$[?(@.event.id == 'a1')].event.text").asArray().containsExactly("Merhaba Kafka");
    }

    @Test
    void brokenRecordsGoToTheDeadLetterTopic() throws Exception {
        template.send(Topics.MESSAGES, "bad-json", "not json at all").get();
        template.send(Topics.MESSAGES, "no-text", "{\"id\":\"no-text\",\"text\":\"\"}").get();

        awaitUntil(() -> received.deadLetters().stream().filter(d -> d.key().equals("bad-json") || d.key().equals("no-text"))
                .count() == 2);

        assertThat(received.deadLetters()).filteredOn(d -> d.key().equals("bad-json")).singleElement()
                .satisfies(deadLetter -> {
                    assertThat(deadLetter.value()).isEqualTo("not json at all");
                    assertThat(deadLetter.originalTopic()).isEqualTo(Topics.MESSAGES);
                    assertThat(deadLetter.reason()).contains("not a valid message");
                });
        assertThat(received.messages()).noneMatch(m -> m.event().id().equals("no-text"));
        assertThat(mvc.get().uri("/messages/dead-letters")).hasStatusOk();
    }

    private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("Condition not met within 30 seconds");
            }
            Thread.sleep(100);
        }
    }
}
