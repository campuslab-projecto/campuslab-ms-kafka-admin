
package cl.duoc.campuslab.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/kafka/topics")
public class KafkaTopicManagementController {

    private final AdminClient admin;
    private final String adminKey;

    public KafkaTopicManagementController(
            AdminClient admin,
            @Value("${KAFKA_ADMIN_API_KEY:}") String adminKey) {

        this.admin = admin;
        this.adminKey = adminKey;
    }

    public record CreateTopicRequest(
            String name,
            Integer partitions,
            Short replicationFactor
    ) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> createTopic(
            @RequestHeader(
                    value = "X-Admin-Key",
                    required = false
            ) String key,
            @RequestBody CreateTopicRequest request
    ) throws Exception {

        if (adminKey.isBlank() ||
                !Objects.equals(adminKey, key)) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Acceso administrativo denegado"
            );
        }

        String name = request.name();

        if (name == null ||
                !name.matches("campuslab\\.[a-z0-9._-]{3,100}") ||
                name.equals("campuslab.booking.events") ||
                name.equals("campuslab.occupancy.events")) {

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Nombre de topico invalido o protegido"
            );
        }

        int partitions = request.partitions() == null
                ? 1 : request.partitions();

        short replicas = request.replicationFactor() == null
                ? 1 : request.replicationFactor();

        if (partitions < 1 || partitions > 6 || replicas != 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Usar entre 1 y 6 particiones y 1 replica"
            );
        }

        NewTopic topic = new NewTopic(
                name, partitions, replicas
        );

        admin.createTopics(List.of(topic))
                .all()
                .get(15, TimeUnit.SECONDS);

        return Map.of(
                "created", name,
                "partitions", partitions,
                "replicationFactor", replicas
        );
    }
}
