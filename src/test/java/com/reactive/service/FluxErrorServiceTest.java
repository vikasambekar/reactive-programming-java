//package com.reactive.service;
//
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import reactor.core.publisher.Mono;
//
//class FluxErrorServiceTest {
//
//    FluxErrorService service;
//
//    @BeforeEach
//    void setup() {
//        service = new FluxErrorService();
//    }
//
//    @Test
//    void getUserById() {
//        Mono<String> userById = service.getUserById("404").log();
//
//        StepVerifier.create(userById)
//                .expectErrorMessage("User not found")
//                .verify();
//    }
//
//    @Test
//    void onErrorReturn() {
//        StepVerifier.create(service.onErrorReturn("404"))
//                .expectNext("I am the fallback value")
//                .verifyComplete();
//    }
//
//    @Test
//    void fallbackShouldBeUsed() {
//        StepVerifier.create(service.getUserWithFallback("404"))
//                .expectNext("DEFAULT_USER")
//                .verifyComplete();
//    }
//
//    @Test
//    void getUserWithMappedError(){
//        StepVerifier.create(service.getUserWithMappedError("404").log())
//                .expectError(IllegalStateException.class)
//                .verify();
//    }
//
//    @Test
//    void getUserWithMappedError1(){
//        StepVerifier.create(service.getUserWithMappedError("404").log())
//                .expectErrorMessage("Service failure")
//                .verify();
//    }
//
//    @Test
//    void getUserWithLogging(){
//        StepVerifier.create(service.getUserWithLogging("404").log())
//                .expectErrorMessage("User not found")
//                .verify();
//    }
//
//    @Test
//    void retryShouldHappen() {
//        StepVerifier.create(service.getUserWithRetry())
//                .expectError(RuntimeException.class)
//                .verify();
//    }
//
//    @Test
//    void timeoutShouldFallback() {
//        StepVerifier.create(service.getUserWithTimeout().log())
//                .expectNext("TIMEOUT_FALLBACK")
//                .verifyComplete();
//    }
//
//    @Test
//    void fluxShouldEmitThenError() {
//        StepVerifier.create(service.getUsersFlux())
//                .expectNext("A", "B", "C")
//                .expectError(RuntimeException.class)
//                .verify();
//    }
//
//
//}