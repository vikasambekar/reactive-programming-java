package com.reactive.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class PersonProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PersonProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<SendResult<String, Object>> send(String topic, String key, Object payload) {
        return Mono.fromFuture(
                        kafkaTemplate.send(topic, key, payload)
                )
                .doOnSuccess(result ->
                {
                    assert result != null;
                    System.out.println("Sent to partition: " + result.getRecordMetadata().partition() +
                            " at: " + result.getRecordMetadata().timestamp());
                })
                .doOnError(error -> System.err.println("Kafka send failed: " + error.getMessage()));
    }
}
