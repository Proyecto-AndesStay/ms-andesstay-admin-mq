package com.andesstay.adminmq.controller;

import com.andesstay.adminmq.dto.QueueRequestDto;
import com.andesstay.adminmq.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("/api/admin/queues")
public class QueueAdminController {

    private final RabbitAdminService adminService;

    public QueueAdminController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> createQueue(@Valid @RequestBody QueueRequestDto dto) {
        boolean created = adminService.createQueue(dto.getName(), dto.isDurable());

        Map<String, Object> response = new HashMap<>();
        if (created) {
            response.put("status", "SUCCESS");
            response.put("message", "Cola creada correctamente");
            response.put("queue", dto.getName());
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } else {
            response.put("status", "ERROR");
            response.put("message", "No fue posible crear la cola");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Map<String, Object>> deleteQueue(@PathVariable String name) {
        boolean deleted = adminService.deleteQueue(name);

        Map<String, Object> response = new HashMap<>();
        if (deleted) {
            response.put("status", "SUCCESS");
            response.put("message", "Cola eliminada correctamente");
            response.put("queue", name);
            return ResponseEntity.ok(response);
        } else {
            response.put("status", "NOT_FOUND");
            response.put("message", "La cola no existe o no pudo ser eliminada");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> getQueueInfo(@PathVariable String name) {
        Properties properties = adminService.getQueueProperties(name);
        if (properties == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("message", "Cola no encontrada"));
        }
        return ResponseEntity.ok(properties);
    }
}