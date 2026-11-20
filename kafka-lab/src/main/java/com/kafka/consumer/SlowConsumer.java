package com.kafka.consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SlowConsumer {
    @KafkaListener(topics="sensor-topic", groupId="analytics-group", concurrency="1")
    public void listen(String value) throws InterruptedException {
    Thread.sleep(10);}

}
