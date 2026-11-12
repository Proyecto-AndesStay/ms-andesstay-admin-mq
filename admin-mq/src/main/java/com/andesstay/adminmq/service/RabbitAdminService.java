package com.andesstay.adminmq.service;

import com.andesstay.adminmq.dto.BindingRequestDto;
import com.andesstay.adminmq.dto.ExchangeRequestDto;
import com.andesstay.adminmq.dto.QueueRequestDto;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class RabbitAdminService {

    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminService(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    public void createQueue(QueueRequestDto dto) {
        Map<String, Object> args = new HashMap<>();
        if (dto.getDlqName() != null && !dto.getDlqName().isBlank()) {
            Queue dlq = new Queue(dto.getDlqName().trim(), true, false, false);
            rabbitAdmin.declareQueue(dlq);
            args.put("x-dead-letter-exchange", "");
            args.put("x-dead-letter-routing-key", dto.getDlqName().trim());
        }

        Queue queue = new Queue(dto.getName().trim(), dto.isDurable(), false, dto.isAutoDelete(), args);
        rabbitAdmin.declareQueue(queue);
    }

    public boolean deleteQueue(String name) {
        return rabbitAdmin.deleteQueue(name.trim());
    }

    public void createExchange(ExchangeRequestDto dto) {
        Exchange exchange;
        String type = dto.getType() != null ? dto.getType().toLowerCase().trim() : "topic";
        String name = dto.getName().trim();

        switch (type) {
            case "direct":
                exchange = new DirectExchange(name, dto.isDurable(), false);
                break;
            case "fanout":
                exchange = new FanoutExchange(name, dto.isDurable(), false);
                break;
            case "topic":
            default:
                exchange = new TopicExchange(name, dto.isDurable(), false);
                break;
        }
        rabbitAdmin.declareExchange(exchange);
    }

    public boolean deleteExchange(String name) {
        return rabbitAdmin.deleteExchange(name.trim());
    }

    public void createBinding(BindingRequestDto dto) {
        Binding binding = new Binding(
                dto.getQueueName().trim(),
                Binding.DestinationType.QUEUE,
                dto.getExchangeName().trim(),
                dto.getRoutingKey() != null ? dto.getRoutingKey().trim() : "",
                null
        );
        rabbitAdmin.declareBinding(binding);
    }
}