package com.flink.study.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutputTagTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> input = env.fromElements(1, 2, 3);

        final OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        SingleOutputStreamOperator<Integer> ds = input
                .process(new ProcessFunction<Integer, Integer>() {

                    @Override
                    public void processElement(
                            Integer value,
                            Context ctx,
                            Collector<Integer> out) throws Exception {
                        // emit data to regular output
                        out.collect(value);

                        // emit data to side output
                        ctx.output(outputTag, "sideout-" + String.valueOf(value));
                    }
                });

        ds.print();
        ds.getSideOutput(outputTag).print();

        try {
            env.execute("OutputTagTest");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
