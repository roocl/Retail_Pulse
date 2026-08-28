package com.retailpulse.consumer;

import com.retailpulse.common.CommerceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CommerceEventListener {
    private static final Logger log = LoggerFactory.getLogger(CommerceEventListener.class);

    @KafkaListener(topics = "${retailpulse.kafka.topic}")
    public void onEvent(ConsumerRecord<String, CommerceEvent> record) {
        log.info("received key={} partition={} offset={} event={}",
                record.key(), record.partition(), record.offset(), record.value());
    }
}
