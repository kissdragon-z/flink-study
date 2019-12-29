package com.flink.study.local;

import com.flink.study.order.OrderEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class OutPutTagTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<String> orderTag = new OutputTag<>("orderTag");

        URL resource = OutPutTagTest.class.getResource("order.csv");

        SingleOutputStreamOperator<String> process = env.readTextFile(resource.getPath())
                .process(new ProcessFunction<String, String>() {

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                        OrderEvent orderEvent = new OrderEvent();

                        String[] split = value.split(",");
                        orderEvent.setOrderNo(split[0]);

                        ctx.output(orderTag, value);

                    }
                });

        process.print();
        process.getSideOutput(orderTag).print();

        try {
            env.execute("OutPutTagTest");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
