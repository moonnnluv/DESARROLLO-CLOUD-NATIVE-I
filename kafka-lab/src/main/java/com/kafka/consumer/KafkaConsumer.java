package com.kafka.consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    @KafkaListener(topics="tutorial-particiones",
                   groupId="mi-grupo-educativo")
    public void listen(ConsumerRecord<String,String> record) {
        System.out.printf("RECIBIDO | particion=%d | offset=%d | key=%s | valor=%s%n",
            record.partition(), record.offset(), record.key(), record.value());
    }
}