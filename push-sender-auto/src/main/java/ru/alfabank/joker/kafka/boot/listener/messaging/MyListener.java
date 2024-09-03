package ru.alfabank.joker.kafka.boot.listener.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.alfabank.joker.kafka.boot.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.service.PushService;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyListener {
    private final PushService pushService;

    @KafkaListener(topics = {"my-topic"}, filter = "otpFilterStrategy")
    public void listen(OtpDto otpDto, ConsumerRecordMetadata metadata) {
        log.info("Received from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());
        this.pushService.sendPush(otpDto);
        log.info("Processed from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());
    }

//    @KafkaListener(topics = {"my-topic"})
//    public void listen(OtpDto otpDto,
//            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//            @Header(KafkaHeaders.OFFSET) int offset
//            ) {
//        log.info("Received from {}-{}-{}", topic, partition, offset);
//        this.pushService.sendPush(otpDto);
//        log.info("Processed from {}-{}-{}", topic, partition, offset);
//    }
//
//    @KafkaListener(topics = {"my-topic"})
//    public void listen(Message<OtpDto> message,
//                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//                       ConsumerRecordMetadata metadata
//    ) {
//        log.info("Received from {}-{}-{}", topic, metadata.partition(), message.getHeaders().get(KafkaHeaders.OFFSET));
//        this.pushService.sendPush(message.getPayload());
//        log.info("Processed from {}-{}-{}", topic, metadata.partition(), message.getHeaders().get(KafkaHeaders.OFFSET));
//    }
}
