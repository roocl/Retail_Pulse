package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CommerceEventProducerTest {
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, CommerceEvent> template = mock(KafkaTemplate.class);

    private GenerationProperties config(long count, long interval) {
        return new GenerationProperties(count, interval, 42, 0, 0, Instant.EPOCH);
    }

    private void acknowledge() {
        when(template.send(eq("test"), anyString(), any(CommerceEvent.class))).thenAnswer(invocation -> {
            CommerceEvent event = invocation.getArgument(2);
            assertEquals(event.userId(), invocation.getArgument(1));
            return CompletableFuture.completedFuture(new SendResult<>(
                    new ProducerRecord<>("test", event.userId(), event),
                    new RecordMetadata(new TopicPartition("test", 0), 0, 0, 0, 0, 0)));
        });
    }

    @Test
    void sendsConfiguredCountUsingUserKey() throws Exception {
        acknowledge();
        new CommerceEventProducer(template, "test", config(3, 0)).run();
        verify(template, times(3)).send(eq("test"), anyString(), any(CommerceEvent.class));
    }

    @Test
    void failedAcknowledgmentStopsWithoutApplicationRetry() {
        when(template.send(eq("test"), anyString(), any(CommerceEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker failed")));
        assertThrows(ExecutionException.class,
                () -> new CommerceEventProducer(template, "test", config(3, 0)).run());
        verify(template, times(1)).send(eq("test"), anyString(), any(CommerceEvent.class));
    }

    @Test
    void synchronousSendFailureStopsImmediately() {
        when(template.send(eq("test"), anyString(), any(CommerceEvent.class)))
                .thenThrow(new IllegalStateException("metadata unavailable"));
        assertThrows(IllegalStateException.class,
                () -> new CommerceEventProducer(template, "test", config(3, 0)).run());
        verify(template, times(1)).send(eq("test"), anyString(), any(CommerceEvent.class));
    }

    @Test
    void continuousModeStopsOnInterruptAndPreservesFlag() {
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class,
                    () -> new CommerceEventProducer(template, "test", config(0, 0)).run());
            assertTrue(Thread.currentThread().isInterrupted());
            verifyNoInteractions(template);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void respectsMinimumIntervalBetweenAcknowledgments() throws Exception {
        acknowledge();
        long started = System.nanoTime();
        new CommerceEventProducer(template, "test", config(3, 25)).run();
        assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(50));
    }
}
