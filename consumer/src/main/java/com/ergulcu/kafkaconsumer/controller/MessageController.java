// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.controller;

import com.ergulcu.kafkaconsumer.service.ReceivedMessages;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Shows what the listener has seen, so the demo can be followed without a Kafka UI. */
@RestController
@RequestMapping("/messages")
public class MessageController {

    private final ReceivedMessages received;

    public MessageController(ReceivedMessages received) {
        this.received = received;
    }

    @GetMapping
    public List<ReceivedMessages.Received> messages() {
        return received.messages();
    }

    @GetMapping("/dead-letters")
    public List<ReceivedMessages.DeadLetter> deadLetters() {
        return received.deadLetters();
    }
}
