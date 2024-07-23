package ru.alfabank.joker.kafka.boot.listener.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.alfabank.joker.kafka.boot.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.service.PushService;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyListener {
    private final PushService pushService;

    @KafkaListener(topics = {"my-topic"})
    public void listen(ConsumerRecord<String, OtpDto> record) {
        log.info("Received from {}-{}-{}", record.topic(), record.partition(), record.offset());
        this.pushService.sendPush(record.value());
        log.info("Processed from {}-{}-{}", record.topic(), record.partition(), record.offset());
    }
}
