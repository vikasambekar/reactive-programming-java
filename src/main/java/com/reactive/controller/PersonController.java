package com.reactive.controller;

import com.reactive.enity.Person;
import com.reactive.repository.PersonRepository;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/persons")
public class PersonController {


    private final PersonRepository personRepository;

    public PersonController(PersonRepository personRepository) {
        this.personRepository = personRepository;
    }

    @PostMapping
    Mono<Person> createPerson(@RequestBody Person person) {
        return personRepository.save(person);
    }

    @GetMapping
    Flux<Person> getPerson() {
        return personRepository.findAll();
    }
}
