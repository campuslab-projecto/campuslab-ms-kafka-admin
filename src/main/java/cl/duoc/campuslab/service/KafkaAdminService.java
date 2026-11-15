
package cl.duoc.campuslab.service;

import cl.duoc.campuslab.model.CreateTopicRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaAdminService {

    private final AdminClient admin;

    public KafkaAdminService(AdminClient admin) {
        this.admin = admin;
    }

    // 1. Listar los topicos disponibles
    public List<String> listTopics() throws Exception {

        return admin.listTopics()
                .names()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .sorted()
                .toList();
    }

    // 2. Consultar particiones y replicas
    public Map<String, Object> describeTopic(
            String name) throws Exception {

        TopicDescription topic = admin
                .describeTopics(List.of(name))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(name);

        List<Map<String, Object>> partitions =
                new ArrayList<>();

        topic.partitions().forEach(p -> {

            Map<String, Object> info =
                    new LinkedHashMap<>();

            info.put("partition", p.partition());

            info.put(
                    "leader",
                    p.leader() == null
                            ? -1
                            : p.leader().id()
            );

            info.put(
                    "replicas",
                    p.replicas().stream()
                            .map(node -> node.id())
                            .toList()
            );

            info.put(
                    "isr",
                    p.isr().stream()
                            .map(node -> node.id())
                            .toList()
            );

            partitions.add(info);
        });

        Map<String, Object> result =
                new LinkedHashMap<>();

        result.put("name", topic.name());
        result.put("partitionCount", partitions.size());
        result.put("partitions", partitions);

        return result;
    }

    // 3. Listar los grupos de consumidores
    public List<String> listGroups() throws Exception {

        return admin.listConsumerGroups()
                .all()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .map(group -> group.groupId())
                .sorted()
                .toList();
    }

    // 4. Crear un nuevo topico
    public Map<String, Object> createTopic(
            CreateTopicRequest request) throws Exception {

        if (request == null || request.name() == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Debe indicar el nombre del topico"
            );
        }

        String name = request.name();

        // Solo permitir topicos de demostracion
        if (!name.matches("campuslab\\.demo\\.[a-z0-9._-]{3,100}")) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Solo se permiten topicos campuslab.demo.*"
            );
        }

        // Proteger los topicos reales de CampusLab
        if (name.equals("campuslab.booking.events") ||
                name.equals("campuslab.occupancy.events")) {

            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Topico protegido"
            );
        }

        int partitions = request.partitions() == null
                ? 1
                : request.partitions();

        short replicas = request.replicationFactor() == null
                ? 1
                : request.replicationFactor();

        // Validar particiones y factor de replicacion
        if (partitions < 1 || partitions > 6 || replicas != 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Usar entre 1 y 6 particiones y 1 replica"
            );
        }

        NewTopic topic = new NewTopic(
                name,
                partitions,
                replicas
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
