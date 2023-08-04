package com.example.webfluxkafkaproducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class WebfluxKafkaProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebfluxKafkaProducerApplication.class, args);
	}

	@Bean
	KafkaSender<String, String> kafkaSender(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
		Map<String, Object> producerProps = new HashMap<>();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		SenderOptions<String, String> senderOptions =
				SenderOptions.<String, String>create(producerProps)
						.maxInFlight(1024);

		return KafkaSender.create(senderOptions);
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> onStart() {
		return event -> {
			//If you have any onStart activity to perform
		};
	}


}

@Component
@EnableScheduling
@Slf4j
@RequiredArgsConstructor
class KafkaProducer {
	final KafkaSender sender;
	@Value("${spring.kafka.topicName}") String topic;

	@Scheduled(fixedRate = 20000)
	void sendMessage() {
		var timestamp = LocalDateTime.now().atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

		Flux<SenderRecord<String, String, Integer>> outboundFlux =
				Flux.range(1, 10)
						.map(i -> SenderRecord
								.create(
										topic,
										0,
										timestamp, LocalDateTime.now().format(DateTimeFormatter.ISO_DATE),
										"Message_" + i,
										i
								)
						);

		sender.send(outboundFlux)
				.doOnError(e-> log.error("Send failed", e))
				.doOnEach(signal -> log.info("message sent successfully"))
				//.doOnNext(r -> System.out.printf("Message #%d send response: %s\n", r.correlationMetadata(), r.recordMetadata()))
				.subscribe();
	}
}
