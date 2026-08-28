package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class SampleEventProducer implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(SampleEventProducer.class);

    private final KafkaTemplate<String, CommerceEvent> kafkaTemplate;
    private final String topic;

    public SampleEventProducer(
            KafkaTemplate<String, CommerceEvent> kafkaTemplate,
            @Value("${retailpulse.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void run(String... args) throws Exception {
        for (CommerceEvent event : SampleEvents.all()) {
            String key = event.userId();
            RecordMetadata metadata = kafkaTemplate.send(topic, key, event)
                    .get(10, TimeUnit.SECONDS)
                    .getRecordMetadata();
            log.info("sent key={} partition={} offset={} event={}",
                    key, metadata.partition(), metadata.offset(), event);
        }
        kafkaTemplate.flush();
        log.info("sent {} sample events to topic {}", SampleEvents.all().size(), topic);
    }
}
