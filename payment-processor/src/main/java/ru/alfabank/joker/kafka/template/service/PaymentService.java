package ru.alfabank.joker.kafka.template.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.alfabank.joker.kafka.template.dto.OtpDto;

import java.time.LocalDateTime;

import static ru.alfabank.joker.kafka.template.configuration.KafkaTemplateConfiguration.MY_TOPIC;


@Service
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, OtpDto> kafkaTemplate;

    public void acceptPayment() {
        OtpDto otpDto = this.preparePayment();
        this.sendPushAsync(otpDto);
    }


    private void sendPushAsync(OtpDto otpDto) {
        kafkaTemplate.sendDefault(otpDto);
    }


//    private void sendPushAsync(OtpDto otpDto) {
//        kafkaTemplate.send(MY_TOPIC ,otpDto);
//    }

//    private void sendPushAsync(OtpDto otpDto) {
//        ProducerRecord<String, OtpDto> producerRecord;
//        producerRecord = new ProducerRecord<>(MY_TOPIC, otpDto);
//        kafkaTemplate.send(producerRecord);
//    }
//
//    private void sendPushAsync(OtpDto otpDto) {
//        Message<OtpDto> message = MessageBuilder.withPayload(otpDto)
//                .setHeader(KafkaHeaders.TOPIC, MY_TOPIC)
//                .setHeader(KafkaHeaders.PARTITION, 0)
//                .setHeader(KafkaHeaders.KEY, "my-key")
//                .build();
//
//        kafkaTemplate.send(message);
//    }

    private static OtpDto preparePayment() {
        return OtpDto.builder()
                .sender("payment-processor")
                .userId("me")
                .code("my-secret-code")
                .expireTime(LocalDateTime.now().plusMinutes(1))
                .build();
    }
}
