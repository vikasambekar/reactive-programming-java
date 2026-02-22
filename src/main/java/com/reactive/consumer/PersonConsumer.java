package com.reactive.consumer;

import com.reactive.enity.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PersonConsumer {

    @KafkaListener(topics = "test-topic", groupId = "reactive-group")
    public void listen(Person event, Acknowledgment ack) {
        System.out.println("Received: " + event);
        ack.acknowledge();
    }
}
