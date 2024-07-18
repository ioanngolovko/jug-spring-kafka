package ru.alfabank.joker.kafka.listener.service;

import org.springframework.stereotype.Service;
import ru.alfabank.joker.kafka.listener.dto.OtpDto;
import ru.alfabank.joker.kafka.listener.exceptions.FireBaseAccountLockedException;
import ru.alfabank.joker.kafka.listener.exceptions.FireBaseUnavailableException;

import java.util.Objects;

@Service
public class PushService {
    public void sendPush(OtpDto otpDto) {
        switch (otpDto.sender()) {
            case "locked-sender": throw new FireBaseAccountLockedException();
            case "far-sender": throw new FireBaseUnavailableException();
            case "unknown-sender": throw new RuntimeException("Basta");
        }
    }
}
