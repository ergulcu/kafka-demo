// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.service;

import com.ergulcu.kafkaconsumer.config.ConsumerProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageListenerTest {

    private final ReceivedMessages received = new ReceivedMessages(
            new ConsumerProperties("demo.messages", 2, Duration.ofMillis(10), 3));
    private final MessageListener listener = new MessageListener(JsonMapper.builder().build(), received);

    @Test
    void storesAValidMessage() {
        listener.onMessage(record("""
                {"id":"1","text":"merhaba","createdAt":"2026-01-01T10:00:00Z"}"""));

        assertThat(received.messages()).singleElement().satisfies(message -> {
            assertThat(message.event().text()).isEqualTo("merhaba");
            assertThat(message.event().createdAt()).isEqualTo(Instant.parse("2026-01-01T10:00:00Z"));
            assertThat(message.offset()).isEqualTo(7);
        });
    }

    @Test
    void ignoresUnknownFields() {
        listener.onMessage(record("{\"id\":\"1\",\"text\":\"ok\",\"extra\":true}"));

        assertThat(received.messages()).hasSize(1);
    }

    @Test
    void rejectsRecordsThatAreNotJson() {
        assertThatThrownBy(() -> listener.onMessage(record("this is not json")))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("not a valid message");
        assertThat(received.messages()).isEmpty();
    }

    @Test
    void rejectsMessagesWithoutText() {
        assertThatThrownBy(() -> listener.onMessage(record("{\"id\":\"1\",\"text\":\"  \"}")))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessage("The message has no text");
        assertThatThrownBy(() -> listener.onMessage(record(null))).isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void keepsOnlyTheNewestMessages() {
        for (int i = 1; i <= 5; i++) {
            listener.onMessage(record("{\"id\":\"" + i + "\",\"text\":\"m" + i + "\"}"));
        }

        assertThat(received.messages()).extracting(message -> message.event().id()).containsExactly("5", "4", "3");
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("demo.messages", 0, 7, "key", value);
    }
}
