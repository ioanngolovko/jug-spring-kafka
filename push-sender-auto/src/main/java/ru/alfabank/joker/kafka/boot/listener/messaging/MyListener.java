package ru.alfabank.joker.kafka.boot.listener.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import ru.alfabank.joker.kafka.boot.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.dto.ResultDto;
import ru.alfabank.joker.kafka.boot.listener.service.PushService;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyListener {
    private final PushService pushService;

    @SendTo("second-topic")
    @KafkaListener(topics = {"my-topic"}, filter = "otpFilterStrategy")
    public ResultDto listen(OtpDto otpDto, ConsumerRecordMetadata metadata) {
        log.info("Received from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());
        this.pushService.sendPush(otpDto);
        log.info("Processed from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());

        return new ResultDto(otpDto.userId(), otpDto.code());
    }
}
