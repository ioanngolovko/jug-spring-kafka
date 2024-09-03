package ru.alfabank.joker.kafka.boot.template.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.alfabank.joker.kafka.boot.template.dto.OtpDto;

import java.time.LocalDateTime;


@Service
@RequiredArgsConstructor
public class PaymentService {

//    For DeserializationException
//    private final KafkaTemplate<String, Object> kafkaTemplate;
//    private void sendPushAsync(OtpDto otpDto) {
//        kafkaTemplate.sendDefault(1);
//    }

    private final KafkaTemplate<String, Object> kafkaTemplate;
    public void acceptPayment() {
        Message<OtpDto> otpDtoMessage = MessageBuilder.withPayload(preparePayment()).build();
        this.sendPushAsync(otpDtoMessage);
    }

    private void sendPushAsync(Message<OtpDto> message) {
        kafkaTemplate.send(message);
    }

    private OtpDto preparePayment() {
        return OtpDto.builder()
                .sender("payment-processor") // good example
                //.sender("far-sender") // for FireBaseUnavailableException
                //.sender("unknown-sender") // for RuntimeException
                //.sender("locked-sender") // for FireBaseAccountLockedException
                .userId("me")
                .code("my-secret-code")
                .expireTime(LocalDateTime.now().plusMinutes(1))
                .build();
    }
}
