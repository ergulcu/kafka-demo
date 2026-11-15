// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.controller;

import com.ergulcu.kafka.contract.MessageEvent;
import com.ergulcu.kafkaproducer.service.MessagePublishException;
import com.ergulcu.kafkaproducer.service.MessageService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@WebMvcTest(MessageController.class)
class MessageControllerTest {

    @Autowired
    private MockMvcTester mvc;

    @MockitoBean
    private MessageService messageService;

    @Test
    void plainTextBodyIsPublished() {
        when(messageService.sendMessage("merhaba"))
                .thenReturn(new MessageEvent("42", "merhaba", Instant.parse("2026-01-01T10:00:00Z")));

        assertThat(mvc.post().uri("/message").contentType(MediaType.TEXT_PLAIN).content("merhaba"))
                .hasStatus(HttpStatus.ACCEPTED)
                .bodyJson().extractingPath("$.id").isEqualTo("42");
    }

    @Test
    void jsonBodyIsPublished() {
        when(messageService.sendMessage(anyString()))
                .thenAnswer(call -> new MessageEvent("1", call.getArgument(0), Instant.now()));

        assertThat(mvc.post().uri("/messages").contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"selam\"}"))
                .hasStatus(HttpStatus.ACCEPTED)
                .bodyJson().extractingPath("$.text").isEqualTo("selam");
        verify(messageService).sendMessage("selam");
    }

    @Test
    void blankMessagesAreRejected() {
        assertThat(mvc.post().uri("/message").contentType(MediaType.TEXT_PLAIN).content("   "))
                .hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(mvc.post().uri("/messages").contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"\"}"))
                .hasStatus(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(messageService);
    }

    @Test
    void anUnavailableBrokerIsReportedAs503() {
        when(messageService.sendMessage(anyString()))
                .thenThrow(new MessagePublishException("Kafka did not accept the message", new RuntimeException()));

        assertThat(mvc.post().uri("/message").contentType(MediaType.TEXT_PLAIN).content("x"))
                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
