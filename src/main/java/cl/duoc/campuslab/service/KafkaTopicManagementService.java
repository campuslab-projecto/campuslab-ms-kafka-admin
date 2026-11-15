
package cl.duoc.campuslab.service;

import cl.duoc.campuslab.model.UpdatePartitionsRequest;
import cl.duoc.campuslab.model.UpdateTopicConfigRequest;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaTopicManagementService {

    private final AdminClient admin;

    public KafkaTopicManagementService(AdminClient admin) {
        this.admin = admin;
    }

    // Solo permitimos modificar topicos de demostracion.
    private void validateDemoTopic(String name) {
        if (name == null ||
                !name.matches("campuslab\\.demo\\.[a-z0-9._-]{3,100}")) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Solo se pueden modificar topicos campuslab.demo.*"
            );
        }
    }

    // 1. Consultar configuraciones de un topico.
    public Map<String, Object> getTopicConfig(
            String name) throws Exception {

        if (name == null || name.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Indicar nombre del topico"
            );
        }

        ConfigResource resource = new ConfigResource(
                ConfigResource.Type.TOPIC, name
        );

        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);

        Map<String, String> settings = new LinkedHashMap<>();

        Set<String> allowed = Set.of(
                "retention.ms",
                "cleanup.policy",
                "min.insync.replicas"
        );

        for (ConfigEntry entry : config.entries()) {
            if (allowed.contains(entry.name())
                    && !entry.isSensitive()) {
                settings.put(entry.name(), entry.value());
            }
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("topic", name);
        response.put("config", settings);

        return response;
    }

    // 2. Actualizar retencion y politica de limpieza.
    public Map<String, String> updateTopicConfig(
            String name,
            UpdateTopicConfigRequest request) throws Exception {

        validateDemoTopic(name);

        if (request == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Indicar configuracion"
            );
        }

        List<AlterConfigOp> operations = new ArrayList<>();

        if (request.retentionMs() != null) {
            long retention = request.retentionMs();

            if (retention < 60000L ||
                    retention > 2592000000L) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Retencion entre 1 minuto y 30 dias"
                );
            }

            operations.add(new AlterConfigOp(
                    new ConfigEntry(
                            "retention.ms",
                            String.valueOf(retention)
                    ),
                    AlterConfigOp.OpType.SET
            ));
        }

        if (request.cleanupPolicy() != null) {
            String policy = request.cleanupPolicy();

            if (!policy.equals("delete") &&
                    !policy.equals("compact")) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Politica valida: delete o compact"
                );
            }

            operations.add(new AlterConfigOp(
                    new ConfigEntry("cleanup.policy", policy),
                    AlterConfigOp.OpType.SET
            ));
        }

        if (operations.isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "No se solicitaron cambios"
            );
        }

        ConfigResource resource = new ConfigResource(
                ConfigResource.Type.TOPIC, name
        );

        Map<ConfigResource, Collection<AlterConfigOp>> changes =
                Map.of(resource, operations);

        admin.incrementalAlterConfigs(changes)
                .all()
                .get(15, TimeUnit.SECONDS);

        return Map.of("updated", name);
    }

    // 3. Aumentar cantidad de particiones.
    public Map<String, Object> updatePartitions(
            String name,
            UpdatePartitionsRequest request) throws Exception {

        validateDemoTopic(name);

        if (request == null ||
                request.partitions() == null ||
                request.partitions() < 1 ||
                request.partitions() > 6) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Indicar entre 1 y 6 particiones"
            );
        }

        TopicDescription description = admin
                .describeTopics(List.of(name))
                .allTopicNames()
                .get(15, TimeUnit.SECONDS)
                .get(name);

        int current = description.partitions().size();

        if (request.partitions() <= current) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "La nueva cantidad debe ser mayor a la actual"
            );
        }

        admin.createPartitions(Map.of(
                name,
                NewPartitions.increaseTo(request.partitions())
        )).all().get(15, TimeUnit.SECONDS);

        return Map.of(
                "updated", name,
                "partitions", request.partitions()
        );
    }

    // 4. Eliminar un topico de demostracion.
    public void deleteTopic(
            String name,
            String confirmation) throws Exception {

        validateDemoTopic(name);

        if (!name.equals(confirmation)) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "La confirmacion debe coincidir con el nombre"
            );
        }

        admin.deleteTopics(List.of(name))
                .all()
                .get(15, TimeUnit.SECONDS);
    }
}
