package com.kafka.controller;
import java.util.concurrent.TimeUnit;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
    public class KafkaController {
        private final KafkaTemplate<String,String> template;
        public KafkaController(KafkaTemplate<String,String> template) {
        this.template = template;
    }
    @GetMapping("/health")
    public String health() { return "API Kafka operativa"; }
    
    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam String message,
        @RequestParam(required=false) String key) throws Exception {
    var result = template.send("tutorial-particiones", key, message)
                         .get(15, TimeUnit.SECONDS);
    var md = result.getRecordMetadata();
    return ResponseEntity.ok("Publicado: particion=" + md.partition()
        + ", offset=" + md.offset() + ", key=" + key
        + ", message=" + message);
    }
}