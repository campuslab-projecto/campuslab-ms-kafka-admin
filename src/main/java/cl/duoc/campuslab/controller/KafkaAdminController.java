
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

    // ==========================================
    // 1. LISTAR TOPICOS
    // ==========================================

    @GetMapping("/topics")
    public List<String> listTopics() throws Exception {

        return kafkaAdminService.listTopics();
    }

    // ==========================================
    // 2. CONSULTAR DETALLE DEL TOPICO
    // ==========================================

    @GetMapping("/topics/{name}")
    public Map<String, Object> describeTopic(
            @PathVariable String name) throws Exception {

        return kafkaAdminService.describeTopic(name);
    }

    // ==========================================
    // 3. LISTAR GRUPOS DE CONSUMIDORES
    // ==========================================

    @GetMapping("/groups")
    public List<String> listGroups() throws Exception {

        return kafkaAdminService.listGroups();
    }

    // ==========================================
    // 4. MONITOREAR OFFSETS DE TOPICOS DLT
    // ==========================================

    @GetMapping("/dlt/offsets")
    public Map<String, Object> getDltOffsets()
            throws Exception {

        return kafkaAdminService.getDltOffsets();
    }
}
