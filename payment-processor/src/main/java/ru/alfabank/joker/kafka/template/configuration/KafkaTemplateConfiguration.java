package ru.alfabank.joker.kafka.template.configuration;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.alfabank.joker.kafka.template.dto.OtpDto;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaTemplateConfiguration {

    public static final String MY_TOPIC = "my-topic";

    @Bean
    @SneakyThrows
    public ProducerFactory<String, OtpDto> producerFactory(MeterRegistry meterRegistry) {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory producerFactory = new DefaultKafkaProducerFactory<>(props);
        producerFactory.addListener(new MicrometerProducerListener<String, OtpDto>(meterRegistry));
        return producerFactory;
    }

    @Bean
    public KafkaTemplate<String, OtpDto> kafkaTemplate(ProducerFactory<String, OtpDto> producerFactory) {
        KafkaTemplate<String, OtpDto> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic(MY_TOPIC);
        return kafkaTemplate;
    }
}
