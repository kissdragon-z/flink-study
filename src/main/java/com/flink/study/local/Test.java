package com.flink.study.local;

public class Test {

    public static void main(String[] args) {

        System.out.println(System.currentTimeMillis() / 1000);

        System.out.println(Test.class.getResource("/order.csv"));

    }
}
