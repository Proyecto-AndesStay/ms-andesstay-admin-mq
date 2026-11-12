package com.andesstay.adminmq.controller;

import com.andesstay.adminmq.dto.BindingRequestDto;
import com.andesstay.adminmq.dto.ExchangeRequestDto;
import com.andesstay.adminmq.dto.QueueRequestDto;
import com.andesstay.adminmq.service.RabbitAdminService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@CrossOrigin(origins = "*")
public class QueueAdminController {

    private final RabbitAdminService rabbitAdminService;

    public QueueAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping("/queues")
    public ResponseEntity<?> createQueue(@RequestBody QueueRequestDto dto) {
        if (dto == null || dto.getName() == null || dto.getName().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "El nombre de la cola no puede estar vacío."));
        }
        rabbitAdminService.createQueue(dto);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Cola creada exitosamente", "queue", dto.getName().trim()));
    }

    @DeleteMapping("/queues/{name}")
    public ResponseEntity<?> deleteQueue(@PathVariable String name) {
        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "Debe indicar un nombre de cola válido para eliminar."));
        }
        boolean deleted = rabbitAdminService.deleteQueue(name);
        return ResponseEntity.ok(Map.of("message", "Operación completada", "queue", name, "deleted", deleted));
    }

    @PostMapping("/exchanges")
    public ResponseEntity<?> createExchange(@RequestBody ExchangeRequestDto dto) {
        if (dto == null || dto.getName() == null || dto.getName().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "El nombre del exchange no puede estar vacío."));
        }
        rabbitAdminService.createExchange(dto);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Exchange creado exitosamente", "exchange", dto.getName().trim()));
    }

    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<?> deleteExchange(@PathVariable String name) {
        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "Debe indicar un nombre de exchange válido."));
        }
        boolean deleted = rabbitAdminService.deleteExchange(name);
        return ResponseEntity.ok(Map.of("message", "Exchange eliminado", "exchange", name, "deleted", deleted));
    }

    @PostMapping("/bindings")
    public ResponseEntity<?> createBinding(@RequestBody BindingRequestDto dto) {
        if (dto == null || dto.getQueueName() == null || dto.getQueueName().isBlank()
                || dto.getExchangeName() == null || dto.getExchangeName().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "queueName y exchangeName son obligatorios para crear un binding."));
        }
        rabbitAdminService.createBinding(dto);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Binding creado exitosamente"));
    }
}