
package cl.duoc.campuslab.service;

import cl.duoc.campuslab.model.CreateTopicRequest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaAdminService {

    private final AdminClient admin;
    private final String bootstrapServers;

    public KafkaAdminService(
            AdminClient admin,
            @Value("${campuslab.kafka.bootstrap-servers}")
            String bootstrapServers) {

        this.admin = admin;
        this.bootstrapServers = bootstrapServers;
    }

    // ==========================================
    // 1. LISTAR TOPICOS
    // ==========================================

    public List<String> listTopics() throws Exception {

        return admin.listTopics()
                .names()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .sorted()
                .toList();
    }

    // ==========================================
    // 2. DESCRIBIR UN TOPICO
    // ==========================================

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

    // ==========================================
    // 3. LISTAR GRUPOS DE CONSUMIDORES
    // ==========================================

    public List<String> listGroups() throws Exception {

        return admin.listConsumerGroups()
                .all()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .map(group -> group.groupId())
                .sorted()
                .toList();
    }

    // ==========================================
    // 4. CREAR TOPICO DE DEMOSTRACION
    // ==========================================

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
        if (!name.matches(
                "campuslab\\.demo\\.[a-z0-9._-]{3,100}"
        )) {
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

        if (partitions < 1 ||
                partitions > 6 ||
                replicas != 1) {

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Usar entre 1 y 6 particiones y 1 replica"
            );
        }

        NewTopic topic = new NewTopic(
                name,
                partitions,
                replicas
        ).configs(Map.of("min.insync.replicas", "1"));

        admin.createTopics(List.of(topic))
                .all()
                .get(15, TimeUnit.SECONDS);

        return Map.of(
                "created", name,
                "partitions", partitions,
                "replicationFactor", replicas
        );
    }

    // ==========================================
    // 5. MONITOREAR TOPICOS DLT
    // ==========================================

    public Map<String, Object> getDltOffsets()
            throws Exception {

        // Solo observar los DLT de CampusLab
        List<String> dltTopics = listTopics()
                .stream()
                .filter(name ->
                        name.startsWith("campuslab.") &&
                                name.endsWith(".DLT")
                )
                .toList();

        Map<String, Object> result =
                new LinkedHashMap<>();

        result.put("topicCount", dltTopics.size());
        result.put("metric", "offset-span");

        // Si no hay DLT, no crear consumidor
        if (dltTopics.isEmpty()) {
            result.put("totalEstimatedRecords", 0L);
            result.put("topics", List.of());
            return result;
        }

        // Obtener particiones reales desde Kafka
        Map<String, TopicDescription> descriptions =
                admin.describeTopics(dltTopics)
                        .allTopicNames()
                        .get(10, TimeUnit.SECONDS);

        List<TopicPartition> topicPartitions =
                new ArrayList<>();

        for (String name : dltTopics) {

            TopicDescription description =
                    descriptions.get(name);

            for (var partition : description.partitions()) {

                topicPartitions.add(
                        new TopicPartition(
                                name,
                                partition.partition()
                        )
                );
            }
        }

        // Consumidor dedicado exclusivamente a
        // consultar offsets. No utiliza grupos.
        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        properties.put(
                ConsumerConfig.CLIENT_ID_CONFIG,
                "campuslab-dlt-offset-monitor"
        );

        properties.put(
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                10000
        );

        List<Map<String, Object>> topicResults =
                new ArrayList<>();

        long totalEstimatedRecords = 0L;

        try (KafkaConsumer<byte[], byte[]> consumer =
                     new KafkaConsumer<>(properties)) {

            // Sin subscribe(), poll() ni commits
            Map<TopicPartition, Long> beginningOffsets =
                    consumer.beginningOffsets(
                            topicPartitions,
                            Duration.ofSeconds(10)
                    );

            Map<TopicPartition, Long> endOffsets =
                    consumer.endOffsets(
                            topicPartitions,
                            Duration.ofSeconds(10)
                    );

            for (String topicName : dltTopics) {

                List<Map<String, Object>> partitions =
                        new ArrayList<>();

                long topicEstimatedRecords = 0L;

                TopicDescription description =
                        descriptions.get(topicName);

                for (var partitionInfo :
                        description.partitions()) {

                    TopicPartition partition =
                            new TopicPartition(
                                    topicName,
                                    partitionInfo.partition()
                            );

                    long beginning =
                            beginningOffsets.get(partition);

                    long end =
                            endOffsets.get(partition);

                    long estimated =
                            Math.max(0L, end - beginning);

                    Map<String, Object> partitionResult =
                            new LinkedHashMap<>();

                    partitionResult.put(
                            "partition",
                            partition.partition()
                    );

                    partitionResult.put(
                            "beginningOffset",
                            beginning
                    );

                    partitionResult.put(
                            "endOffset",
                            end
                    );

                    partitionResult.put(
                            "estimatedRecords",
                            estimated
                    );

                    partitions.add(partitionResult);

                    topicEstimatedRecords += estimated;
                }

                Map<String, Object> topicResult =
                        new LinkedHashMap<>();

                topicResult.put("name", topicName);

                topicResult.put(
                        "partitionCount",
                        partitions.size()
                );

                topicResult.put(
                        "estimatedRecords",
                        topicEstimatedRecords
                );

                topicResult.put(
                        "partitions",
                        partitions
                );

                topicResults.add(topicResult);

                totalEstimatedRecords +=
                        topicEstimatedRecords;
            }
        }

        result.put(
                "totalEstimatedRecords",
                totalEstimatedRecords
        );

        result.put("topics", topicResults);

        return result;
    }
}
