package com.zml.flink.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @GetMapping("/testControlelr")
    public String testControlelr() {


        return "test";
    }
}
