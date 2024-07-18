package ru.alfabank.joker.kafka.listener.configuration;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
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
            MessageListener<String, OtpDto> listener,
            KafkaTemplate<byte[], byte[]> template) {

        ContainerProperties containerProperties = new ContainerProperties(MY_TOPIC);
        containerProperties.setMessageListener(listener);

        var listenerContainer = new KafkaMessageListenerContainer<>(factory, containerProperties);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer);

        listenerContainer.setCommonErrorHandler(errorHandler);

        return listenerContainer;
    }


    @Bean
    @SneakyThrows
    public ProducerFactory<byte[], byte[]> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<byte[], byte[]> kafkaTemplate(ProducerFactory<byte[], byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
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
