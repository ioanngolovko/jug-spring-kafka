package ru.alfabank.joker.kafka.boot.listener.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import ru.alfabank.joker.kafka.boot.dto.GiveMeYourMoneyDto;
import ru.alfabank.joker.kafka.boot.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.dto.ResultDto;
import ru.alfabank.joker.kafka.boot.listener.service.PushService;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = {"my-topic"})
public class MyListener {
    private final PushService pushService;

    @KafkaHandler
    @SendTo("second-topic")
    public ResultDto listen(OtpDto otpDto, ConsumerRecordMetadata metadata) {
        log.info("Received from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());
        this.pushService.sendPush(otpDto);
        log.info("Processed from {}-{}-{}", metadata.topic(), metadata.partition(), metadata.offset());

        return new ResultDto(otpDto.userId(), otpDto.code());
    }

    @KafkaHandler
    public void listen(GiveMeYourMoneyDto dto) {
        log.info("Give me your money!");
    }

    @KafkaHandler(isDefault = true)
    public void listenDefault(String str) {
        log.info("What the hell are you? {}", str);
    }
}
