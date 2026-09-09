package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@EnableConfigurationProperties(GenerationProperties.class)
public class CommerceEventProducer implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CommerceEventProducer.class);
    private final KafkaTemplate<String, CommerceEvent> kafkaTemplate;
    private final String topic;
    private final GenerationProperties properties;

    public CommerceEventProducer(KafkaTemplate<String, CommerceEvent> kafkaTemplate,
                                 @Value("${retailpulse.kafka.topic}") String topic,
                                 GenerationProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws Exception {
        CommerceEventGenerator generator = new CommerceEventGenerator(properties);
        long sent = 0;
        long duplicates = 0;
        long outOfOrder = 0;
        log.info("generation started config={}", properties);
        try {
            while (properties.count() == 0 || sent < properties.count()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("event generation interrupted");
                }
                var generated = generator.next();
                CommerceEvent event = generated.event();
                RecordMetadata metadata = kafkaTemplate.send(topic, event.userId(), event)
                        .get(35, TimeUnit.SECONDS).getRecordMetadata();
                sent++;
                if (generated.duplicate()) duplicates++;
                if (generated.outOfOrder()) outOfOrder++;
                log.info("sent key={} partition={} offset={} duplicate={} outOfOrder={} event={}",
                        event.userId(), metadata.partition(), metadata.offset(),
                        generated.duplicate(), generated.outOfOrder(), event);
                if (properties.intervalMs() > 0 && (properties.count() == 0 || sent < properties.count())) {
                    Thread.sleep(properties.intervalMs());
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error("generation interrupted after {} acknowledged records", sent);
            throw exception;
        } catch (Exception exception) {
            log.error("generation failed after {} acknowledged records; last send may have reached Kafka; stopping",
                    sent, exception);
            throw exception;
        }
        log.info("generation completed sent={} duplicates={} outOfOrder={} topic={}",
                sent, duplicates, outOfOrder, topic);
    }
}
