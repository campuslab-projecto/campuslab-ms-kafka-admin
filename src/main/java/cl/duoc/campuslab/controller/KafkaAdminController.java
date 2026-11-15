
package cl.duoc.campuslab.controller;

import cl.duoc.campuslab.service.KafkaAdminService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(
            KafkaAdminService kafkaAdminService) {

        this.kafkaAdminService = kafkaAdminService;
    }

    // 1. Listar los topicos de Kafka
    @GetMapping("/topics")
    public List<String> listTopics() throws Exception {

        return kafkaAdminService.listTopics();
    }

    // 2. Consultar particiones y replicas
    @GetMapping("/topics/{name}")
    public Map<String, Object> describeTopic(
            @PathVariable String name) throws Exception {

        return kafkaAdminService.describeTopic(name);
    }

    // 3. Listar grupos de consumidores
    @GetMapping("/groups")
    public List<String> listGroups() throws Exception {

        return kafkaAdminService.listGroups();
    }
}
