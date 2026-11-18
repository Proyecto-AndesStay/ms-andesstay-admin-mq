package com.andesstay.adminmq.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/kafka-admin")
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final AdminClient adminClient;

    public KafkaAdminController(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // 1. Listar todos los topicos y su detalle (particiones, replicas, lideres)
    @GetMapping("/topics")
    public ResponseEntity<Object> listTopics() {
        try {
            Set<String> topicNames = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(topicNames).allTopicNames().get(5, TimeUnit.SECONDS);

            List<Object> result = new ArrayList<>();
            for (TopicDescription desc : descriptions.values()) {
                Map<String, Object> topicInfo = new HashMap<>();
                topicInfo.put("name", desc.name());
                topicInfo.put("partitions", desc.partitions().size());
                int replicas = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                topicInfo.put("replicationFactor", replicas);
                result.add(topicInfo);
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // 2. Crear un nuevo topico con particiones, replicas y retencion
    @PostMapping("/topics")
    public ResponseEntity<Object> createTopic(@RequestBody Map<String, Object> body) {
        try {
            String name = (String) body.get("name");
            int partitions = body.get("partitions") != null ? Integer.parseInt(body.get("partitions").toString()) : 3;
            short replicas = body.get("replicas") != null ? Short.parseShort(body.get("replicas").toString()) : 3;
            String retentionMs = body.get("retentionMs") != null ? body.get("retentionMs").toString() : "604800000";

            NewTopic newTopic = new NewTopic(name, partitions, replicas);
            Map<String, String> configs = new HashMap<>();
            configs.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs);
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
            newTopic.configs(configs);

            adminClient.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "message", "Topico creado exitosamente",
                    "topic", name,
                    "partitions", partitions,
                    "replicas", replicas,
                    "retentionMs", retentionMs
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // 3. Modificar configuracion de un topico (ej. retention.ms)
    @PutMapping("/topics/{topicName}/config")
    public ResponseEntity<Object> updateTopicConfig(@PathVariable String topicName, @RequestBody Map<String, String> body) {
        try {
            String retentionMs = body.getOrDefault("retentionMs", "604800000");
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            AlterConfigOp op = new AlterConfigOp(
                    new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, retentionMs),
                    AlterConfigOp.OpType.SET
            );
            Collection<AlterConfigOp> ops = Collections.singleton(op);
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, ops);

            adminClient.incrementalAlterConfigs(configs).all().get(5, TimeUnit.SECONDS);

            return ResponseEntity.ok(Map.of(
                    "message", "Configuracion de topico actualizada",
                    "topic", topicName,
                    "retentionMs", retentionMs
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // 4. Eliminar un topico
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Object> deleteTopic(@PathVariable String topicName) {
        try {
            adminClient.deleteTopics(Collections.singleton(topicName)).all().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of("message", "Topico eliminado correctamente", "topic", topicName));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // 5. Inspeccionar Consumer Groups activos del proyecto
    @GetMapping("/consumer-groups")
    @SuppressWarnings("deprecation")
    public ResponseEntity<Object> listConsumerGroups() {
        try {
            List<String> groupIds = Arrays.asList("cg-andesstay-reportes", "cg-andesstay-auditoria");
            Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds).all().get(5, TimeUnit.SECONDS);

            List<Object> result = new ArrayList<>();
            for (ConsumerGroupDescription desc : descriptions.values()) {
                Map<String, Object> info = new HashMap<>();
                info.put("groupId", desc.groupId());
                info.put("state", desc.groupState().toString());
                info.put("membersCount", desc.members().size());
                info.put("coordinator", desc.coordinator() != null ? desc.coordinator().host() + ":" + desc.coordinator().port() : "N/A");
                result.add(info);
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}