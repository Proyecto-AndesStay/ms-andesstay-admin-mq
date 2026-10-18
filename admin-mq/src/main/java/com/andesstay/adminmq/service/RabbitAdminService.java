package com.andesstay.adminmq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);
    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminService(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    public boolean createQueue(String queueName, boolean durable) {
        try {
            Queue queue = new Queue(queueName, durable, false, false);
            String declaredName = rabbitAdmin.declareQueue(queue);
            log.info("Cola declarada exitosamente en RabbitMQ: {}", declaredName);
            return declaredName != null;
        } catch (Exception e) {
            log.error("Error al declarar la cola {}: {}", queueName, e.getMessage());
            return false;
        }
    }

    public boolean deleteQueue(String queueName) {
        try {
            boolean deleted = rabbitAdmin.deleteQueue(queueName);
            log.info("Eliminación de la cola {}: {}", queueName, deleted);
            return deleted;
        } catch (Exception e) {
            log.error("Error al eliminar la cola {}: {}", queueName, e.getMessage());
            return false;
        }
    }

    public Properties getQueueProperties(String queueName) {
        return rabbitAdmin.getQueueProperties(queueName);
    }
}