package com.reactive.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.util.List;

class FluxServiceTest {

    FluxService fluxService;

    @BeforeEach
    void setup() {
        fluxService = new FluxService();
    }

    @Test
    @DisplayName("Verify the flux data using StepVerifier")
    void getStepVerifierFlux() {
        StepVerifier.create(fluxService.getFlux())
                .expectNext("Vikas", "Virat", "Rohit", "Hardik")
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux data")
    void getFlux() {
        fluxService.getFlux().doOnNext((data) -> {
            System.out.println("Data--" + data);
        }).subscribe();
    }

    @Test
    @DisplayName("Verify the flux iterable data")
    void getFluxIterableData() {
        fluxService.fluxFromIterable().doOnNext((data) -> {
            System.out.println("Data--" + data);
        }).subscribe();
    }

    @Test
    @DisplayName("Verify the map usage")
    void getFluxMapData() {
        Flux<String> stringFlux = fluxService.fluxConvertToUpperCase();
        StepVerifier.create(stringFlux)
                .expectNext("Vikas".toUpperCase(), "Virat".toUpperCase(), "Rohit".toUpperCase(), "Hardik".toUpperCase())
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux usage")
    void getFluxToMonoData() {
        Flux<String> stringFlux = fluxService.monoToFlux();
        StepVerifier.create(stringFlux)
                .expectNext("Apple")
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux filter usage")
        // Here we are using the mono hence it will only return the single element
    void getMonoFilterData() {
        Mono<String> filterFlux = fluxService.fluxtoMono();

        StepVerifier.create(filterFlux)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux filter usage")
        // Here we are using the flux hence it will return the numbers of elements which are there
    void getFluxFilterData() {
        Flux<String> filterFlux = fluxService.ifExampleUsingFlux(4);

        StepVerifier.create(filterFlux)
                .expectNextCount(4)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux defaultIfEmptyExample usage")
    void defaultIfEmptyExample() {
        Flux<String> filterFlux = fluxService.defaultIfEmptyExample(8);

        StepVerifier.create(filterFlux)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux switchIfEmptyExample usage")
    void switchIfEmptyExample() {
        Flux<String> filterFlux = fluxService.switchIfEmptyExample(8);

        StepVerifier.create(filterFlux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux concat usage")
    void concatFlux() {
        Flux<String> concatFlux = fluxService.concatFlux().log();

        StepVerifier.create(concatFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux merge usage")
    void mergeFlux() {
        Flux<String> concatFlux = fluxService.mergeFlux().log();

        StepVerifier.create(concatFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux zip usage")
    void zipFlux() {
        Flux<Tuple2<String, Integer>> tuple2Flux = fluxService.zipFlux().log();

        StepVerifier.create(tuple2Flux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("Verify the flux zip usage")
    void zipFlux2() {
        Flux<String> tuple2Flux = fluxService.zipFlux2().log();

        StepVerifier.create(tuple2Flux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("Side effecft methods")
    void sideEffectMethods() {
        fluxService.sideEffectMethods().log().subscribe(data -> {
            System.out.println(data);
        });
    }
}