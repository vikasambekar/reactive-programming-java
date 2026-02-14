package com.reactive.enity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.relational.core.mapping.Table;

@Table("person")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String id;
    private String name;
    private int age;
}