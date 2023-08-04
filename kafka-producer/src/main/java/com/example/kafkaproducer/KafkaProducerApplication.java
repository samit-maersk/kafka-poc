package com.example.kafkaproducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@SpringBootApplication
public class KafkaProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}


	@Bean
	ApplicationListener<ApplicationReadyEvent> onStart() {
		return event -> {

		};
	}

}

@Component
@Slf4j
@RequiredArgsConstructor
@EnableScheduling
class KafkaProducerClient {
	final KafkaTemplate<String, String> kafkaTemplate;
	@Value("${spring.kafka.topicName}") String topic;
	@Scheduled(fixedRate = 10000)
	void produce() {
		log.info("Message sent");
		var timestamp = LocalDateTime.now().atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
		kafkaTemplate
				.send(
						topic,
						0,
						timestamp,
						LocalDateTime.now().toString(),
						"Message -%s".formatted(timestamp)
				);
	}
}