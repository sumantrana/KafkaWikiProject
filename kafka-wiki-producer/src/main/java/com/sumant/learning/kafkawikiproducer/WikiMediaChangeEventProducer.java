package com.sumant.learning.kafkawikiproducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class WikiMediaChangeEventProducer {

    private final String WIKI_CHANGES_TOPIC = "wiki-changes";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikiMediaChangeEventProducer(KafkaTemplate<String, String> template){
        this.kafkaTemplate = template;
    }

    public void sendMessage(String message){
        ProducerRecord<String, String> record = new ProducerRecord<>(WIKI_CHANGES_TOPIC, message);
        kafkaTemplate.send(record);
    }
}
