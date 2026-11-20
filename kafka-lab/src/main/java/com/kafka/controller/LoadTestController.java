package com.kafka.controller;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/test")
public class LoadTestController {
    private final KafkaTemplate<String, String> kafka;
        public LoadTestController(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
        }
        @PostMapping("/start")
        public ResponseEntity<String> start(@RequestParam(defaultValue="1000") int count) {
        if (count < 1 || count > 100000) {
        return ResponseEntity.badRequest().body("count debe estar entre 1 y 100000");
        }
        long start = System.nanoTime();
        int confirmed = 0;
        int failed = 0;
        // Ventanas acotadas: no acumular 100.000 futuros simultáneos.
        for (int base = 0; base < count; base += 500) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = base; i < Math.min(base + 500, count); i++) {
        String key = "sensor-" + (i % 5);
        String value = "Sensor-Data-" + i + "-Payload-IoT";
        futures.add(kafka.send("sensor-topic", key, value));
        }
        for (CompletableFuture<?> future : futures) {
        try { future.join(); confirmed++; }
        catch (Exception ex) { failed++; }
        }
        }
        double seconds = Math.max((System.nanoTime()-start)/1e9, 0.000001);
        return ResponseEntity.ok(String.format(java.util.Locale.US,
        "Solicitados=%d Confirmados=%d Fallidos=%d Segundos=%.3f Throughput=%.2f msg/s",
        count, confirmed, failed, seconds, confirmed/seconds));
        }
    
}
