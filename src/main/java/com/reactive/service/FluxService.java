package com.reactive.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

@Service
public class FluxService {

    // create the flux using the .just() method
    public Flux<String> getFlux() {
        return Flux.just("Vikas", "Virat", "Rohit", "Hardik").log();
    }

    // create the flux using the .fromIterable() method
    public Flux<String> fluxFromIterable() {
        List<String> fruits = List.of("Apple", "Banana", "Cheryy");
        return Flux.fromIterable(fruits).log();
    }

    // convert flux to mono using the .next()
    public Mono<String> fluxtoMono() {
        List<String> fruits = List.of("Apple", "Mango");
        // .next() converts flux to mono
        return Flux.fromIterable(fruits).log().next();
    }

    // convert mono to flux using the .flux() method
    public Flux<String> monoToFlux() {
        // only the single element is allowed
        Mono<String> mono = Mono.just("Apple");
        // Multiple elements are allowed here
        Flux<String> flux = Flux.just("Apple", "Banana", "Cheryy");
        return mono.flux();
    }

    public Flux<Void> emptyFlux() {
        return Flux.empty();
    }

    public Flux<String> fluxConvertToUpperCase() {
        return getFlux().map(String::toUpperCase);
    }

    // transform - transforming the Function<>
    public Flux<String> transform() {
        Function<Flux<String>, Flux<String>> funInterface = (name) -> name.map(String::toUpperCase);
        return getFlux().transform(funInterface).log();
    }

    // convert to the mono (.next())
    public Mono<String> convertToMono(int length) {
        return getFlux().filter(name -> name.length() > length).next();
    }

    // convert to the flux (.next())
    public Flux<String> convertToFlux(int length) {
        return Mono.just("I'm  Mono").flux();
    }

    // filter example : return the flux
    public Flux<String> ifExampleUsingFlux(int length) {
        return getFlux().filter(name -> name.length() > length);
    }

    public Flux<String> flatMapExample() {
        // Inside the flatMap if you pass the mono then it will return the single element
        // Inside the flatmap if you pass the flux then it will return the multiple elements of flux of elements
//        return getFlux().flatMap(data -> Mono.just(data));
        return getFlux().flatMap(data -> Flux.just(data));
    }

    // defaultIfEmpty : This is used to return the fallback value
    public Flux<String> defaultIfEmptyExample(int length) {
        return getFlux()
                .filter(name -> name.length() > length)
                .defaultIfEmpty("I'm fallback here") // fallback value
                .log();
    }

    // switchIfEmpty : This is used to return the fallback publishers (Flux/Mono)
    public Flux<String> switchIfEmptyExample(int length) {
        Flux<String> flux = Flux.just("Apple", "Banana", "Cheryy");
        return getFlux()
                .filter(name -> name.length() > length)
                .switchIfEmpty(flux) // fallback publisher
                .log();
    }

    // Joining two flux - concat(static), concatWith(instance)
    // concat - this is the synchronous way of concatenating the flux (of same type of flux only)
    // Order is maintained here
    public Flux<String> concatFlux() {
        Flux<String> nameFlux = Flux.just("Vikas", "Rohit", "Virat");
        Flux<String> fruitFlux = Flux.just("Apple", "Banana", "Cheryy");

//        return nameFlux.concatWith(fruitFlux);
        return Flux.concat(nameFlux.delayElements(Duration.ofSeconds(1)), fruitFlux.delayElements(Duration.ofSeconds(2)));
    }

    // Joining two flux - merge(static), mergeWith(instance)
    // merge - this is the Asynchronous way of concatenating the flux (of same type of flux only)
    // Order is not maintained here
    public Flux<String> mergeFlux() {
        Flux<String> nameFlux = Flux.just("Vikas", "Rohit", "Virat");
        Flux<String> fruitFlux = Flux.just("Apple", "Banana", "Cheryy");

//        return nameFlux.mergeWith(fruitFlux);
        return Flux.merge(nameFlux.delayElements(Duration.ofSeconds(1)), fluxFromIterable().delayElements(Duration.ofSeconds(2)));
    }

    // Joining the different types of elements
    // zip and zipWith() - This is used to combine different types of flux
    public Flux<Tuple2<String, Integer>> zipFlux() {
        Flux<String> nameFlux = Flux.just("Vikas", "Rohit", "Virat");
        Flux<Integer> indexFlux = Flux.just(1, 2, 3);

//        return nameFlux.zipWith(indexFlux);
        return Flux.zip(nameFlux, indexFlux);
    }

    // using the custom add
    // by default it is used to return the Tuple (array)
    public Flux<String> zipFlux2() {
        Flux<String> nameFlux = Flux.just("Vikas", "Rohit", "Virat");
        Flux<Integer> indexFlux = Flux.just(1, 2, 3);

        return Flux.zip(nameFlux, indexFlux, (first, second) -> {
            return first + " : " + second;
        });
    }


    /* SIDE EFFECT METHODS : These methods execute before actual onNext, onSubscribe */
    public Flux<String> sideEffectMethods() {
        return getFlux().doOnNext(data -> {
            System.out.println(data + " On Next");
        }).doOnSubscribe(data -> {
            System.out.println(data + " on subscribe");
        }).doOnEach(data -> {
            System.out.println(data + " on each");
        });
    }

    // Difference between map, flatMap and flatMapMany using Mono and Flux

    // ------------------------------------------------------------
// 1. map with Mono
// map transforms the value inside Mono
// If the mapper returns a Mono, the result becomes Mono<Mono<T>>
// map does NOT flatten
    public Mono<Mono<String>> mapWithMono() {
//        return Mono.just("vikas")
//                .map(String::toUpperCase); ----> Mono<String>
        return Mono.just("vikas")
                .map(name -> Mono.just(name.toUpperCase()));
    }

    // ------------------------------------------------------------
// 2. flatMap with Mono
// flatMap is used when the mapper returns Mono<R>
// It flattens Mono<Mono<R>> into Mono<R>
    public Mono<String> flatMapWithMono() {
        return Mono.just("vikas")
                .flatMap(data -> Mono.just("1"));
    }

    // ------------------------------------------------------------
// 3. flatMapMany with Mono
// Used when a single value needs to produce multiple values
// Mono<T> -> Flux<R>
    public Flux<String> flatMapManyWithMono() {
        return Mono.just("vikas")
                .flatMapMany(name ->
                        Flux.just("Hello " + name, "Bye " + name)
                );
    }

    // ------------------------------------------------------------
// 4. map with Flux
// map transforms each value in Flux
// If the mapper returns Flux, the result becomes Flux<Flux<T>>
// map does NOT flatten
    public Flux<Flux<String>> mapWithFlux() {
        return Flux.just("vikas")
                .map(data -> Flux.just("1"));
    }

    // ------------------------------------------------------------
// 5. flatMap with Flux
// flatMap flattens inner publishers
// With Flux, the mapper can return Mono<R> or Flux<R>
// Final result is always Flux<R>
    public Flux<String> flatMapWithFlux() {
        return Flux.just("vikas")
                .flatMap(data -> Mono.just("1"));
        // or
        // .flatMap(data -> Flux.just("1"));
    }

    // ------------------------------------------------------------
// 6. flatMapMany after converting Flux to Mono
// collectList converts Flux<T> to Mono<List<T>>
// flatMapMany is then applied on Mono
    public Flux<String> flatMapManyAfterCollectList() {
        return Flux.just("vikas")
                .collectList()
                .flatMapMany(list -> Flux.just("1"));
    }
}

/*
    NOTES :
    1. Map
        - 1 → 1 transformation
    2. FlatMap
        - 1 → async many (or one)
        - Used when function returns Mono / Flux
        - FlatMap always return the Mono or Flux


    3. map, flatMap, filter, doOnNext, etc. are called -----------> Reactive operators
 */
