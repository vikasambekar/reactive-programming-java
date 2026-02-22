package com.reactive.controller;

import com.reactive.enity.Person;
import com.reactive.repository.PersonRepository;
import com.reactive.producer.PersonProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/person")
@RequiredArgsConstructor
public class PersonController {

    private final PersonRepository personRepository;
    private final PersonProducer personProducer;

    private final String TEST_TOPIC = "test-topic";

    @PostMapping
    public Mono<String> createPerson(@RequestBody Person person) {
        return personRepository.save(person)
                .flatMap(savedPerson ->
                        personProducer
                                .send(TEST_TOPIC, savedPerson.getId(), savedPerson)
                                .thenReturn("Event published Successfully!")
                );
    }

    @GetMapping
    Flux<Person> getPerson() {
        return personRepository.findAll();
    }
}
