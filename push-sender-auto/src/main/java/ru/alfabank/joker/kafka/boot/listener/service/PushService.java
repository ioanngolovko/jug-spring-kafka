package ru.alfabank.joker.kafka.boot.listener.service;

import org.springframework.stereotype.Service;
import ru.alfabank.joker.kafka.boot.dto.OtpDto;
import ru.alfabank.joker.kafka.boot.listener.exceptions.FireBaseUnavailableException;

@Service
public class PushService {
    public void sendPush(OtpDto otpDto) {
        switch (otpDto.sender()) {
            //case "locked-sender": throw new FireBaseAccountLockedException();
            case "far-sender": throw new FireBaseUnavailableException();
            case "unknown-sender": throw new RuntimeException("Basta");
        }
    }
}
