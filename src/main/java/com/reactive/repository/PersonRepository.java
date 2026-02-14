package com.reactive.repository;

import com.reactive.enity.Person;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface PersonRepository extends ReactiveCrudRepository<Person, String> {
}
