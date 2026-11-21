package com.andesstay.adminmq.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:29092,localhost:29093,localhost:29094}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    // Topico 1: Streaming de Reservas (3 particiones, 3 replicas, retencion 7 dias)
    @Bean
    public NewTopic reservasStreamingTopic() {
        return TopicBuilder.name("andesstay-reservas-streaming")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    // Topico 2: Auditoria de Eventos (3 particiones, 3 replicas, compact,delete, retencion 30 dias)
    @Bean
    public NewTopic auditoriaEventosTopic() {
        return TopicBuilder.name("andesstay-auditoria-eventos")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")
                .build();
    }

    // Topico DLT para Auditoria (3 particiones, 3 replicas, retencion 14 dias)
    @Bean
    public NewTopic auditoriaEventosDltTopic() {
        return TopicBuilder.name("andesstay-auditoria-eventos.DLT")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    // Topico DLT para Streaming de Reservas (3 particiones, 3 replicas, retencion 14 dias)
    @Bean
    public NewTopic reservasStreamingDltTopic() {
        return TopicBuilder.name("andesstay-reservas-streaming.DLT")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}