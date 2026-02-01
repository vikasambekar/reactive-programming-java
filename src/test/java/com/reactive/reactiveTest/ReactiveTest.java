package com.reactive.reactiveTest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple4;

import java.time.Duration;
import java.util.List;

public class ReactiveTest {
    @Test
    @DisplayName(value = "Mono tests")
    void monoTest() {
        Mono<String> m1 = Mono
                .just("Hello Vikas, Keep learning")
                .log();

        m1.subscribe(data -> {
            System.out.println("data --->" + data);
        });
    }

    @Test
    @DisplayName(value = "Mono error tests")
    void monoErrorTest() {
        Mono<Object> errorEmitted = Mono.error(new RuntimeException("Mono error emitted")).log();

        errorEmitted.subscribe(data -> {
            System.out.println("data --->" + data);
        });
    }

    @Test
    @DisplayName(value = "Mono zip tests")
    void monoZipTest() {
        List<Integer> intList = List.of(1, 2, 3, 4, 5, 6);
        Mono<String> m1 = Mono.just("This is the first mono here");
        Mono<String> m2 = Mono.just("This is the second mono here");
        Mono<Integer> m3 = Mono.just(1);
        Mono<List<Integer>> m4 = Mono.just(intList);

        Mono<Tuple4<String, String, Integer, List<Integer>>> zip = Mono.zip(m1, m2, m3, m4);
        zip.subscribe(data -> {
            System.out.println("data --->" + data.getT4());
        });
    }

    @Test
    @DisplayName(value = "Mono withZip tests")
    void monoWithZipTest() {
        var m1 = Mono.just("This is the first mono here");
        Mono<String> m2 = Mono.just("This is the second mono here");
        Mono<String> m3 = Mono.just("This is the third mono here");
        Mono<Tuple2<String, String>> zip = Mono.zip(m1, m2);

        Mono<Tuple2<Tuple2<String, String>, String>> tuple2Mono = zip.zipWith(m3);

        tuple2Mono
                .doOnNext(data -> System.out.println("data ---> " + data))
                .subscribe();

    }

    @Test
    @DisplayName(value = "Mono map tests")
    void monoMapTest() {
        Mono<String> m1 = Mono
                .just("Hello Vikas, Keep learning")
                .log();

        // TODO : Map directly changes the data
        Mono<String> map = m1.map(String::toUpperCase);
        map.subscribe(System.out::println);
    }

    @Test
    @DisplayName(value = "Mono flatmap tests")
    void monoFlatMapTest() {
        Mono<String[]> m1 = Mono
                .just("Hello Vikas, Keep learning")
                .log()
                .flatMap(value -> Mono.just(value.split(" ")));

        // TODO : Flatmap - takes the asynchronous map and converts, process it like asynchronously
        // TODO :  Returns the map

        m1.doOnNext(data -> {
            for (String s : data) {
                System.out.println("---->" + s);
            }
        }).subscribe();
    }

    @Test
    @DisplayName(value = "Mono flatMapMany tests")
    void monoFlatMapManyTest() {
        Flux<String> flux = Mono
                .just("Hello Vikas, Keep learning")
                .flatMapMany(value -> Flux.just(value.split(" ")))
                .log();

        // TODO : Flatmap many takes the mono and convert it to the Flux
        // TODO : Flux is nothing but the Iterator of objects (example array of objects)
        // Mono deals with the single element
        // Flux deals with multiple elements

        flux.doOnNext(data -> {
            System.out.println("---->" + data);
        }).subscribe();
    }

    @Test
    @DisplayName(value = "Mono concat with tests")
    void monoConcatWithTest() {
        Mono<String> m1 = Mono.just("Hello, I'm first mono");
        Mono<String> m2 = Mono.just("Hello, I'm second mono");

        Flux<String> stringFlux = m1.concatWith(m2).log();
        // Flux is the string array here
        // The no of elements that many onNext call will happen here
        stringFlux.subscribe(System.out::println);
    }

    @Test
    @DisplayName(value = "Mono delayElements with tests")
    void monoDelayElementsTest() {
        Mono<String> m1 = Mono.just("Hello, I'm first mono");
        Mono<String> m2 = Mono.just("Hello, I'm second mono");

        Flux<String> stringFlux = m1.concatWith(m2).delayElements(Duration.ofMillis(2000)).log();

        stringFlux.subscribe((data) -> {
            // since we have added the delays hence it got shifted to the different thread
            System.out.println(Thread.currentThread().getName());
            System.out.println(data);
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Terminated main thread");
    }
}
