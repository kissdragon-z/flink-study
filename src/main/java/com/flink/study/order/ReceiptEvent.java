package com.flink.study.order;

public class ReceiptEvent {

    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "payOrderNo='" + payOrderNo + '\'' +
                ", channelCode='" + channelCode + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public String getPayOrderNo() {
        return payOrderNo;
    }

    public void setPayOrderNo(String payOrderNo) {
        this.payOrderNo = payOrderNo;
    }

    private String payOrderNo;
    private String channelCode;
    private Long eventTime;

    public String getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(String channelCode) {
        this.channelCode = channelCode;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

}
