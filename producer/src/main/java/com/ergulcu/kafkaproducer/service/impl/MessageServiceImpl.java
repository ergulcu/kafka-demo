// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.service.impl;

import com.ergulcu.kafka.contract.MessageEvent;
import com.ergulcu.kafkaproducer.config.ProducerProperties;
import com.ergulcu.kafkaproducer.service.MessagePublishException;
import com.ergulcu.kafkaproducer.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class MessageServiceImpl implements MessageService {

    private static final Logger log = LoggerFactory.getLogger(MessageServiceImpl.class);

    private final KafkaTemplate<String, MessageEvent> kafkaTemplate;
    private final ProducerProperties properties;
    private final Clock clock = Clock.systemUTC();

    public MessageServiceImpl(KafkaTemplate<String, MessageEvent> kafkaTemplate, ProducerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public MessageEvent sendMessage(String text) {
        MessageEvent event = new MessageEvent(UUID.randomUUID().toString(), text, clock.instant());
        try {
            // The id is the key, so retries of the same event land on the same partition.
            SendResult<String, MessageEvent> result = kafkaTemplate.send(properties.topic(), event.id(), event)
                    .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            log.info("Sent message {} to {}-{}@{}", event.id(), result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            return event;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagePublishException("Interrupted while sending the message", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new MessagePublishException("Kafka did not accept the message", e);
        }
    }
}
