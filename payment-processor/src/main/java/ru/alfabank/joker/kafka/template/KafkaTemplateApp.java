package ru.alfabank.joker.kafka.template;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.alfabank.joker.kafka.template.service.PaymentService;

@SpringBootApplication
@RequiredArgsConstructor
public class KafkaTemplateApp {

    private final PaymentService paymentService;

    public static void main(String[] args) {
        SpringApplication.run(KafkaTemplateApp.class, args).close();
    }

    @PostConstruct
    void doYourJob() {
        for (int i = 0; i < 10; ++i) {
            paymentService.acceptPayment();
        }
    }
}
