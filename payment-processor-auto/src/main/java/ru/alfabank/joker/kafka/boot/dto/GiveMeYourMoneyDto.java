package ru.alfabank.joker.kafka.boot.dto;

import lombok.Builder;

@Builder
public record GiveMeYourMoneyDto(
        String userId,
        String message
) {
}
