package com.reactive.service;

import com.reactive.enity.User;
import com.reactive.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Mono<User> createUser(User user) {

        return userRepository.save(user)
                .doOnSubscribe(sub -> log.debug("Creating user in DB"))
                .doOnSuccess(ObjectUtils::isEmpty)
                .doOnError(error -> log.error("Error occurred while creating user", error));
    }

    public Flux<User> getAllUsers() {
        log.info("Fetching all users");

        return userRepository.findAll()
                .doOnSubscribe(sub -> log.debug("Querying all users from DB"))
                .doOnComplete(() -> log.info("Completed fetching all users"))
                .doOnError(error ->
                        log.error("Error occurred while fetching users", error));
    }

    public Mono<User> getUserById(Long id) {
        log.info("Fetching user by id={}", id);

        return userRepository.findById(id)
                .doOnSubscribe(sub -> log.debug("Querying DB for user id={}", id))
                .doOnSuccess(user -> {
                    if (user != null) {
                        log.info("User found with id={}", id);
                    } else {
                        log.warn("User not found with id={}", id);
                    }
                })
                .doOnError(error ->
                        log.error("Error occurred while fetching user id={}", id, error));
    }

    public Mono<User> getUserByEmail(String email) {
        log.info("Fetching user by email={}", email);

        return userRepository.findByEmail(email)
                .doOnSubscribe(sub -> log.debug("Querying DB for user email= {}", email))
                .doOnSuccess(user -> {
                    if (user != null) {
                        log.info("User found with email={}", email);
                    } else {
                        log.warn("User not found with email={}", email);
                    }
                })
                .doOnError(error ->
                        log.error("Error occurred while fetching user email={}", email, error));
    }

    public Mono<Void> deleteUser(Long id) {
        log.info("Request received to delete user id={}", id);

        return userRepository.deleteById(id)
                .doOnSubscribe(sub -> log.debug("Deleting user id={} from DB", id))
                .doOnSuccess(unused ->
                        log.info("User deleted successfully id={}", id))
                .doOnError(error ->
                        log.error("Error occurred while deleting user id={}", id, error));
    }
}