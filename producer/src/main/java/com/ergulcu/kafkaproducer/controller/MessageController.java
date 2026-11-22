// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.controller;

import com.ergulcu.kafka.contract.MessageEvent;
import com.ergulcu.kafkaproducer.service.MessagePublishException;
import com.ergulcu.kafkaproducer.service.MessageService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    public record MessageRequest(@NotBlank @Size(max = 10_000) String text) {
    }

    private final MessageService messageService;

    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }

    /** The original endpoint: the whole body is the message text. */
    @PostMapping(path = "/message", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<MessageEvent> postMessage(@RequestBody String text) {
        if (text.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(messageService.sendMessage(text));
    }

    /** JSON variant: {@code {"text": "..."}}. */
    @PostMapping(path = "/messages", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<MessageEvent> postJson(@Valid @RequestBody MessageRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(messageService.sendMessage(request.text()));
    }

    @ExceptionHandler(MessagePublishException.class)
    ProblemDetail publishFailed(MessagePublishException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
