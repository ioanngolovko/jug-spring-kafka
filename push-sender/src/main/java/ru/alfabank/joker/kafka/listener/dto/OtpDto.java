package ru.alfabank.joker.kafka.listener.dto;

import lombok.Builder;

import java.time.LocalDateTime;

@Builder
public record OtpDto(
        String sender,
        String userId,
        String code,
        LocalDateTime expireTime
) {
}
