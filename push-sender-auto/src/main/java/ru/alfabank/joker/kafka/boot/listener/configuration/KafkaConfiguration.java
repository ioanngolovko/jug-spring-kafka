package ru.alfabank.joker.kafka.boot.listener.configuration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.*;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import ru.alfabank.joker.kafka.boot.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.exceptions.FireBaseAccountLockedException;
import ru.alfabank.joker.kafka.boot.listener.exceptions.FireBaseUnavailableException;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean
    DefaultKafkaConsumerFactoryCustomizer consumerFactoryCustomizer() {
        return consumerFactory -> {
            JsonDeserializer<OtpDto> jsonDeserializer = new JsonDeserializer<>(OtpDto.class);
            jsonDeserializer.setUseTypeHeaders(false);
            ErrorHandlingDeserializer errorHandlingDeserializer = new ErrorHandlingDeserializer(jsonDeserializer);
            consumerFactory.setValueDeserializer(errorHandlingDeserializer);
        };
    }

    @Bean
    DefaultKafkaProducerFactoryCustomizer serializerCustomizer() {
        Serializer keySerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                String.class, new StringSerializer()
        ));
        Serializer valueSerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                OtpDto.class, new JsonSerializer<>()
        ));

        return producerFactory -> {
            DefaultKafkaProducerFactory<Object, Object> factory
                    = (DefaultKafkaProducerFactory<Object, Object>)producerFactory;
            factory.setKeySerializer(keySerializer);
            factory.setValueSerializer(valueSerializer);
        };
    }

    @Bean
    public CommonErrorHandler commonErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {
        CommonErrorHandler defaultErrorHandler = defaultErrorHandler(kafkaTemplate);
        CommonDelegatingErrorHandler delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultErrorHandler);

        delegatingErrorHandler.setErrorHandlers(errorHandlingDelegates(kafkaTemplate));
        return delegatingErrorHandler;
    }

    private static CommonErrorHandler defaultErrorHandler(
            KafkaTemplate<Object, Object> otpKafkaTemplate
    ) {
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(otpKafkaTemplate),
                new FixedBackOff(0, 2)
        );

        return defaultErrorHandler;
    }


    private CommonErrorHandler serDeErrorHandler(
            KafkaTemplate<Object, Object> deserializationDltTemplate
    ) {
        DeadLetterPublishingRecoverer serDeRecoverer = new DeadLetterPublishingRecoverer(
                deserializationDltTemplate,
                (record, e) -> {
                    // my-topic.serde.DLT
                    return new TopicPartition(String.format("%s.%s.%s", record.topic(), "serde", "DLT"), record.partition());
                });
        return new DefaultErrorHandler(
                serDeRecoverer,
                new FixedBackOff(0, 0)
        );
    }

    private LinkedHashMap<Class<? extends Throwable>, CommonErrorHandler> errorHandlingDelegates(
            KafkaTemplate<Object, Object> deserializationDltTemplate
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



}
