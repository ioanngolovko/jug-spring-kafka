package ru.alfabank.joker.kafka.listener.configuration;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import ru.alfabank.joker.kafka.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.listener.exceptions.FireBaseAccountLockedException;
import ru.alfabank.joker.kafka.listener.exceptions.FireBaseUnavailableException;
import ru.alfabank.joker.kafka.listener.service.PushService;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
            CommonErrorHandler commonErrorHandler) {
        ContainerProperties containerProperties = new ContainerProperties(MY_TOPIC);
        containerProperties.setMessageListener(listener);
        containerProperties.setAckMode(ContainerProperties.AckMode.COUNT_TIME);
        containerProperties.setAckTime(5000);
        containerProperties.setAckCount(20);

        var listenerContainer = new KafkaMessageListenerContainer<>(factory, containerProperties);
        listenerContainer.setCommonErrorHandler(commonErrorHandler);
        return listenerContainer;
    }

    @Bean
    public CommonErrorHandler commonErrorHandler(
            KafkaTemplate<byte[], byte[]> deserializationDltTemplate,
            KafkaTemplate<String, OtpDto> otpKafkaTemplate
    ) {
        CommonErrorHandler defaultErrorHandler = defaultErrorHandler(otpKafkaTemplate);
        CommonDelegatingErrorHandler delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultErrorHandler);

        delegatingErrorHandler.setErrorHandlers(errorHandlingDelegates(deserializationDltTemplate));
        return delegatingErrorHandler;
    }


    private CommonErrorHandler defaultErrorHandler(
            KafkaTemplate<String, OtpDto> otpKafkaTemplate
    ) {
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(otpKafkaTemplate),
                new FixedBackOff(0, 2)
        );

        return defaultErrorHandler;
    }

    private CommonErrorHandler serDeErrorHandler(
            KafkaTemplate<byte[], byte[]> deserializationDltTemplate
    ) {
        DeadLetterPublishingRecoverer serDeRecoverer = new DeadLetterPublishingRecoverer(
                deserializationDltTemplate,
                (record, e) -> {
                    // my-topic.serde.DLT
                    return new TopicPartition(String.format("%s.%s.%s", MY_TOPIC, "serde", "DLT"), record.partition());
                });
        return new DefaultErrorHandler(
                serDeRecoverer,
                new FixedBackOff(0, 0)
        );
    }

    private LinkedHashMap<Class<? extends Throwable>, CommonErrorHandler> errorHandlingDelegates(
            KafkaTemplate<byte[], byte[]> deserializationDltTemplate
    ) {
        LinkedHashMap<Class<? extends Throwable>, CommonErrorHandler> delegates = new LinkedHashMap<>();
        delegates.put(DeserializationException.class, serDeErrorHandler(deserializationDltTemplate));

        ExponentialBackOff exponentialBackOff = new ExponentialBackOff(250, 2);
        exponentialBackOff.setMaxElapsedTime(3000);
        delegates.put(
                FireBaseUnavailableException.class,
                new DefaultErrorHandler(exponentialBackOff)
        );
        delegates.put(
                FireBaseAccountLockedException.class,
                new CommonContainerStoppingErrorHandler()
        );

        return delegates;
    }

    @Bean
    @SneakyThrows
    public ProducerFactory<String, OtpDto> otpProducerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OtpDto> otpKafkaTemplate(ProducerFactory<String, OtpDto> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
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
