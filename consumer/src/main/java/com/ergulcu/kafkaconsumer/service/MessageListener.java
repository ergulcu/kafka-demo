// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.service;

import com.ergulcu.kafka.contract.MessageEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

@Service
public class MessageListener {

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    private final JsonMapper jsonMapper;
    private final ReceivedMessages received;
    private final Clock clock = Clock.systemUTC();

    public MessageListener(JsonMapper jsonMapper, ReceivedMessages received) {
        this.jsonMapper = jsonMapper;
        this.received = received;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        MessageEvent event = parse(record.value());
        log.info("Received message {} from {}-{}@{}: {}", event.id(), record.topic(), record.partition(),
                record.offset(), event.text());
        received.add(new ReceivedMessages.Received(event, record.topic(), record.partition(), record.offset(),
                clock.instant()));
    }

    @KafkaListener(topics = "${app.kafka.topic}" + com.ergulcu.kafka.contract.Topics.DEAD_LETTER_SUFFIX,
            groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String reason = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String originalTopic = header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        log.warn("Dead letter from {}: {} ({})", originalTopic, record.value(), reason);
        received.addDeadLetter(new ReceivedMessages.DeadLetter(record.key(), record.value(), reason, originalTopic,
                clock.instant()));
    }

    MessageEvent parse(String value) {
        if (value == null) {
            throw new InvalidMessageException("The record has no value");
        }
        MessageEvent event;
        try {
            event = jsonMapper.readValue(value, MessageEvent.class);
        } catch (JacksonException e) {
            throw new InvalidMessageException("The record is not a valid message: " + e.getOriginalMessage(), e);
        }
        if (event.text() == null || event.text().isBlank()) {
            throw new InvalidMessageException("The message has no text");
        }
        return event;
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
