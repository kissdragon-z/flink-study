package com.flink.study.test;

import com.flink.study.order.OrderEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutputTagTestV2 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> input = env.fromElements(1, 2, 3);

        final OutputTag<OrderEvent> outputTag = new OutputTag<OrderEvent>("side-output") {
        };

        try {

            SingleOutputStreamOperator<Object> process = input.process(new ProcessFunction<Integer, Object>() {
                @Override
                public void processElement(Integer value, Context ctx, Collector<Object> out) throws Exception {

                    OrderEvent orderEvent = new OrderEvent();

                    orderEvent.setOrderNo(value.toString());

                    ctx.output(outputTag, orderEvent);

                }
            });

            process.print();
            process.getSideOutput(outputTag).print();

            env.execute("OutputTagTestV2");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
