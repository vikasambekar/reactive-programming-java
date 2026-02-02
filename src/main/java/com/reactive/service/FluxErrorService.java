package com.reactive.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
public class FluxErrorService {

    public Mono<String> getUserById(String id) {
        if ("404".equals(id)) {
            return Mono.error(new RuntimeException("User not found"));
        }
        return Mono.just("User Id: 1");
    }

    // This returns the fallback value when encounters the error
    // fallback value as like defaultIfEmpty
    public Mono<String> onErrorReturn(String id) {
        return getUserById(id)
                .onErrorReturn("I am the fallback value");
    }

    // onErrorResume : returns the new publisher (Mono/Flux) fallback mechanism to handle the error
    // fallback publisher as like switchIfEmpty
    public Mono<String> getUserWithFallback(String id) {
        return getUserById(id)
                .onErrorResume(ex -> Mono.just("DEFAULT_USER"));
    }

    // onErrorMap : this is used to translate the error into different error
    public Mono<String> getUserWithMappedError(String id) {
        return getUserById(id)
                .onErrorMap(ex -> new IllegalStateException("Service failure", ex));
    }

    // doOnError : this is the logging side effect
    public Mono<String> getUserWithLogging(String id) {
        return getUserById(id)
                .doOnError(ex -> System.out.println("LOG ERROR: " + ex.getMessage()));
    }

    // FromCallable - will only execute when it gets subscribed (This is the lazy initialization)
    // retry, this will retry number of times it has configured
    public Mono<Object> getUserWithRetry() {
        return Mono.fromCallable(() -> {
                    throw new RuntimeException("Temporary failure");
                })
                .retry(2);
    }

    // timeout : This wil only wait for the given seconds
    // timeout recovery : This gets timeout in 1 second and emits the fallback flow
    public Mono<String> getUserWithTimeout() {
        return Mono.delay(Duration.ofSeconds(50))
                .map(i -> "Vikas")
                .timeout(Duration.ofSeconds(1))
                .onErrorResume(ex -> Mono.just("TIMEOUT_FALLBACK"));
    }

    // This executes in the sequence manner
    // At first flux will execute the "A", "B", "C" then it will execute the error
    public Flux<String> getUsersFlux() {
        return Flux.just("A", "B", "C")
                .concatWith(Flux.error(new RuntimeException("Boom")));
    }
}

/*
    | Operator              | Purpose                         | Used where         |
| --------------------- | ------------------------------- | ------------------ |
| `doOnError`           | Side-effects (logging, metrics) | Everywhere         |
| `onErrorResume`       | Fallback / recovery             | MOST USED          |
| `onErrorReturn`       | Static fallback                 | Simple cases       |
| `onErrorMap`          | Exception translation           | Service boundaries |
| `retry` / `retryWhen` | Transient failures              | IO / network       |
| `timeout`             | Slow downstream                 | External calls     |
| `switchIfEmpty`       | Empty ≠ error                   | DB / cache         |

 */
