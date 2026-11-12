package com.andesstay.adminmq.dto;

public class ExchangeRequestDto {
    private String name;
    private String type = "topic"; // direct, topic, fanout
    private boolean durable = true;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }
}