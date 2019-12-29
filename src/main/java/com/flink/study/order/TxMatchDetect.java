package com.flink.study.order;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxMatchDetect {

    static final OutputTag<OrderEvent> unMatchOrders = new OutputTag<OrderEvent>("unMatchOrders") {

    };//没有找到对应的支付信息
    static final OutputTag<ReceiptEvent> unmatchReceipts = new OutputTag<ReceiptEvent>("unmatchReceipts") {
    };//没有找到对应的订单信息

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //step1: 获取order的事件流
        URL orderResource = TxMatchDetect.class.getResource("/order.csv");

        KeyedStream orderEventStream = env.readTextFile(orderResource.getPath())
                .map(data -> {

                    String[] split = data.split(",");

                    OrderEvent orderEv = new OrderEvent();

                    orderEv.setOrderNo(split[0]);
                    orderEv.setPayOrderNo(split[1]);
                    orderEv.setEventTime(Long.valueOf(split[2]));

                    return orderEv;

                }).filter(new FilterFunction<OrderEvent>() {

                    //过滤支付单不为空，则为真正的进行支付过的数据
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {

                        return null != orderEvent.getOrderNo() && "" != orderEvent.getPayOrderNo();
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {

                    //TODO 按照事件流中的数据作为

                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                }).keyBy("payOrderNo");

        URL payres = TxMatchDetect.class.getResource("/receipt.csv");

        //读取支付的信息流
        KeyedStream receiptEventStream = env.readTextFile(payres.getPath())
                .map(data -> {
                    String[] split = data.split(",");

                    ReceiptEvent receiptEvent = new ReceiptEvent();

                    receiptEvent.setPayOrderNo(split[0]);
                    receiptEvent.setChannelCode(split[1]);
                    receiptEvent.setEventTime(Long.valueOf(split[2]));

                    return receiptEvent;
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {

                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                }).keyBy("payOrderNo");

        SingleOutputStreamOperator process = orderEventStream.connect(receiptEventStream)
                .process(new CoProcessFunction<OrderEvent, ReceiptEvent, Object>() {

                    ValueState<OrderEvent> orderState = null;
                    ValueState<ReceiptEvent> receiptState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        orderState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
                        //
                        receiptState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state", ReceiptEvent.class));
                    }

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context ctx, Collector<Object> out) throws Exception {

                        ReceiptEvent receiptEvent = receiptState.value();
//
                        System.out.println("============================");
                        System.out.println("订单数据流已经到来*****************************");
                        System.out.println(orderEvent.toString());
                        System.out.println("============================");

                        if (null != receiptEvent) {

                            System.out.println("@@@@@@" + receiptEvent.toString());
                            receiptState.clear();

                        } else {
                            System.out.println("update orderEvent");

                            orderState.update(orderEvent);

                            //此处按照订单的时间戳 往后 推迟 5s，实际可以按照当前的系统事件 + 5S的推迟事件
                            ctx.timerService().registerEventTimeTimer(System.currentTimeMillis() + 10);

                        }

                    }

                    @Override
                    public void processElement2(ReceiptEvent receiptEvent, Context ctx, Collector<Object> out) throws Exception {

                        OrderEvent orderEvent = orderState.value();
//
                        System.out.println("---------------------------");
                        System.out.println("支付数据已经到来");
                        System.out.println(receiptEvent.toString());
                        System.out.println("---------------------------");

                        if (null != orderEvent) {

                            orderState.clear();

                        } else {

                            System.out.println("update receiptEvent");

                            receiptState.update(receiptEvent);

                            //此处按照当前事件 + 5s的推迟时间
                            ctx.timerService().registerEventTimeTimer(System.currentTimeMillis() + 5000);

                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        //此处到了定时的时间，以慢的那个为准，在这里可以进行告警处理

                        //step1: 输出到指定流
//
                        if (orderState.value() != null) {
                            ctx.output(TxMatchDetect.unMatchOrders, orderState.value());

                            System.out.println("该订单没有找到对应的支付单：" + orderState.value());

                        }

                        if (receiptState.value() != null) {
                            ctx.output(TxMatchDetect.unmatchReceipts, receiptState.value());
                            System.out.println("没有找到对应的订单信息：" + receiptState.value());

                        }
//
//                        //step2： 清空数据
                        orderState.clear();
                        receiptState.clear();

                    }
                });

        process.print();
        process.getSideOutput(TxMatchDetect.unMatchOrders).print();
        process.getSideOutput(TxMatchDetect.unmatchReceipts).print();
        try {
            env.execute("orderPayCheck");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
