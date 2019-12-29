package com.flink.study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> text = env.readTextFile("file:///Users/zhangminglong/Documents/duapp/flink/11.txt");
//
//        text.flatMap((String number, Collector<String> out) -> {
//            StringBuilder builder = new StringBuilder();
//            for (int i = 0; i < Integer.valueOf(number); i++) {
//                builder.append("a" + i);
//                out.collect(builder.toString());
//            }
//        }).returns(Types.STRING).print();

        env.fromElements(1, 2, 3)
                .map(i -> i * i).print();

        DataStreamSource<String> dss = env.readTextFile("", "utf-8");

        dss.flatMap(new FlatMapFunction<String, Object>() {

            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {

            }
        });

        env.execute("123");

    }
}
