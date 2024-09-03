package ru.alfabank.joker.kafka.boot.listener.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import ru.alfabank.joker.kafka.boot.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.exceptions.FireBaseAccountLockedException;
import ru.alfabank.joker.kafka.boot.listener.exceptions.FireBaseUnavailableException;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;

@Slf4j
@Configuration
public class KafkaConfiguration {

    @Bean
    JsonMessageConverter messageConverter(ObjectMapper objectMapper) {
        return new JsonMessageConverter(objectMapper);
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

    @Bean
    public RecordFilterStrategy<String, String> otpFilterStrategy(ObjectMapper objectMapper) {
        return new RecordFilterStrategy<String, String>() {
            @Override
            @SneakyThrows
            public boolean filter(ConsumerRecord<String, String> record) {
                OtpDto otpDto = objectMapper.readValue(record.value(), OtpDto.class);
                boolean shouldBeFiltered = otpDto.expireTime().isBefore(LocalDateTime.now());

                if (shouldBeFiltered) {
                    log.info("Skipping {}-{}-{} due to expiration",
                            record.topic(),
                            record.partition(),
                            record.offset()
                    );
                }

                return shouldBeFiltered;
            }
        };
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
        delegates.put(ConversionException.class, serDeErrorHandler(deserializationDltTemplate));

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
