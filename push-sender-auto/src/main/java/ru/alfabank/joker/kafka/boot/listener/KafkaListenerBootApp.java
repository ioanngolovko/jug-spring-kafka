package ru.alfabank.joker.kafka.boot.listener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaListenerBootApp {
    public static void main(String[] args) {
        SpringApplication.run(KafkaListenerBootApp.class, args);
    }
}
