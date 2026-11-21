package cl.sanos_y_salvos.kafka.api.controller;

import cl.sanos_y_salvos.kafka.api.dto.ConsumerGroupResponse;
import cl.sanos_y_salvos.kafka.api.dto.CreateTopicRequest;
import cl.sanos_y_salvos.kafka.api.dto.TopicDetailResponse;
import cl.sanos_y_salvos.kafka.api.dto.UpdateTopicConfigRequest;
import cl.sanos_y_salvos.kafka.api.service.KafkaService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;



@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    
    private final KafkaService kafkaService;

    public KafkaController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicDetailResponse> createTopic(@Valid @RequestBody CreateTopicRequest request)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.status(HttpStatus.CREATED).body(kafkaService.createTopic(request));
    }
    
    @GetMapping("/topics")
    public ResponseEntity<List<TopicDetailResponse>> listTopics()
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaService.listTopics());
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<TopicDetailResponse> getTopic(@PathVariable String name)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaService.getTopicDetails(name));
    }
    
    @PutMapping("/topics/{name}")
    public ResponseEntity<TopicDetailResponse> updateTopic(
            @PathVariable String name,
            @Valid @RequestBody UpdateTopicConfigRequest request)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaService.updateTopic(name, request));
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, String>> deleteTopic(@PathVariable String name)
            throws ExecutionException, InterruptedException {
        kafkaService.deleteTopic(name);
        return ResponseEntity.ok(Map.of("message", "Tópico eliminado correctamente: " + name));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupResponse>> listConsumerGroups()
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaService.listConsumerGroups());
    }

}
