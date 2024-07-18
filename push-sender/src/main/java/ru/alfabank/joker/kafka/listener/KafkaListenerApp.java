package ru.alfabank.joker.kafka.listener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaListenerApp {
    public static void main(String[] args) {
        SpringApplication.run(KafkaListenerApp.class, args);
    }
}
