package ru.alfabank.joker.kafka.boot.template.config;

import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaTemplateConfiguration {

    @Bean
    DefaultKafkaProducerFactoryCustomizer serializerCustomizer() {
        return producerFactory -> producerFactory.setValueSerializer(new JsonSerializer<>());
    }
}
