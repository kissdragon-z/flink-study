package com.flink.study.order;

public class OrderEvent {

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderNo='" + orderNo + '\'' +
                ", payOrderNo='" + payOrderNo + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    private String orderNo;

    public String getPayOrderNo() {
        return payOrderNo;
    }

    public void setPayOrderNo(String payOrderNo) {
        this.payOrderNo = payOrderNo;
    }

    private String payOrderNo;

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    private Long eventTime;

}
