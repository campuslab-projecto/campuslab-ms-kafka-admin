
package cl.duoc.campuslab.controller;

import cl.duoc.campuslab.model.CreateTopicRequest;
import cl.duoc.campuslab.model.UpdatePartitionsRequest;
import cl.duoc.campuslab.model.UpdateTopicConfigRequest;
import cl.duoc.campuslab.service.KafkaAdminService;
import cl.duoc.campuslab.service.KafkaTopicManagementService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka/topics")
public class KafkaTopicManagementController {

    private final KafkaAdminService kafkaAdminService;
    private final KafkaTopicManagementService managementService;
    private final String adminKey;

    public KafkaTopicManagementController(
            KafkaAdminService kafkaAdminService,
            KafkaTopicManagementService managementService,
            @Value("${KAFKA_ADMIN_API_KEY:}") String adminKey) {

        this.kafkaAdminService = kafkaAdminService;
        this.managementService = managementService;
        this.adminKey = adminKey;
    }

    // Validar acceso administrativo
    private void validateAdminKey(String providedKey) {

        if (adminKey.isBlank() || providedKey == null ||
                !MessageDigest.isEqual(
                        adminKey.getBytes(StandardCharsets.UTF_8),
                        providedKey.getBytes(StandardCharsets.UTF_8))) {

            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Acceso administrativo denegado"
            );
        }
    }

    // 1. Crear un topico
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> createTopic(
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key,
            @RequestBody CreateTopicRequest request
    ) throws Exception {

        validateAdminKey(key);

        return kafkaAdminService.createTopic(request);
    }

    // 2. Consultar configuracion de un topico
    @GetMapping("/{name}/config")
    public Map<String, Object> getTopicConfig(
            @PathVariable String name,
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key
    ) throws Exception {

        validateAdminKey(key);

        return managementService.getTopicConfig(name);
    }

    // 3. Modificar retencion y politica de limpieza
    @PutMapping("/{name}/config")
    public Map<String, String> updateTopicConfig(
            @PathVariable String name,
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key,
            @RequestBody UpdateTopicConfigRequest request
    ) throws Exception {

        validateAdminKey(key);

        return managementService.updateTopicConfig(name, request);
    }

    // 4. Aumentar particiones
    @PatchMapping("/{name}/partitions")
    public Map<String, Object> updatePartitions(
            @PathVariable String name,
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key,
            @RequestBody UpdatePartitionsRequest request
    ) throws Exception {

        validateAdminKey(key);

        return managementService.updatePartitions(name, request);
    }

    // 5. Eliminar un topico de demostracion
    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(
            @PathVariable String name,
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key,
            @RequestParam("confirm") String confirmation
    ) throws Exception {

        validateAdminKey(key);

        managementService.deleteTopic(name, confirmation);
    }
}
