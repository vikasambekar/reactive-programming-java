package com.reactive.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
@RequestMapping("/home")
public class TestClass {

    @RequestMapping("/page1")
    @ResponseBody
    public String home() {
        System.out.println("Home page");
        return "Home";
    }

    // RestController = Controller + ResponseBody
}
