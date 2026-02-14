package com.reactive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
    BEAN
    - DEFINITION : A bean in spring boot is an object that is created, managed and destroyed by the spring container.
    - Think of Spring as a Factory + Manager
        You do not create objects yourself using new
        You tell Spring:
        “Hey Spring, I need this object whenever required”
    -   Spring:
    Creates it
    Keeps it in memory
    Gives it to whoever asks
    Manages its lifecycle
    👉 That managed object is called a Bean.
 */
@Configuration
public class MyConfig {

    @Bean
    public AppConfig appConfig() {
        return new AppConfig();
    }
}
