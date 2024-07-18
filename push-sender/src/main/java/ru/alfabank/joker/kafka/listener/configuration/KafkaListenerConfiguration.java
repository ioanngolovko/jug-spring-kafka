package ru.alfabank.joker.kafka.listener.configuration;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.alfabank.joker.kafka.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.listener.service.PushService;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaListenerConfiguration {

    public static final String MY_TOPIC = "my-topic";

    private final PushService pushService;

    @Bean
    @SneakyThrows
    public ConsumerFactory<String, OtpDto> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JsonDeserializer<OtpDto> jsonDeserializer = new JsonDeserializer<>(OtpDto.class);
        jsonDeserializer.setUseTypeHeaders(false);
        ErrorHandlingDeserializer errorHandlingDeserializer = new ErrorHandlingDeserializer(jsonDeserializer);

        DefaultKafkaConsumerFactory<String, OtpDto> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
        consumerFactory.setValueDeserializer(errorHandlingDeserializer);
        return consumerFactory;
    }

    @Bean
    MessageListener<String, OtpDto> messageListener() {
        return (record) -> {
            log.info("Received from {}-{}-{}", record.topic(), record.partition(), record.offset());
            this.pushService.sendPush(record.value());
            log.info("Processed from {}-{}-{}", record.topic(), record.partition(), record.offset());
        };
    }

    @Bean
    KafkaMessageListenerContainer<String, OtpDto> kafkaListenerContainer(
            ConsumerFactory<String, OtpDto> factory,
            MessageListener<String, OtpDto> listener) {

        ContainerProperties containerProperties = new ContainerProperties(MY_TOPIC);
        containerProperties.setMessageListener(listener);

        return new KafkaMessageListenerContainer<>(factory, containerProperties);
    }

//    @Bean
//    ConcurrentMessageListenerContainer<String, OtpDto> kafkaListenerContainer(
//            ConsumerFactory<String, OtpDto> factory,
//            MessageListener<String, OtpDto> listener
//    ) {
//        ContainerProperties containerProperties = new ContainerProperties(MY_TOPIC);
//        containerProperties.setMessageListener(listener);
//
//        var container = new ConcurrentMessageListenerContainer<>(factory, containerProperties);
//        container.setConcurrency(4);
//
//        return container;
//    }
}
