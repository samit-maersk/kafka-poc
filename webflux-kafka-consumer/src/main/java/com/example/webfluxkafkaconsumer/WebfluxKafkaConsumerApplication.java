package com.example.webfluxkafkaconsumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class WebfluxKafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebfluxKafkaConsumerApplication.class, args);
	}

	@Bean
	KafkaReceiver<String,String> kafkaReceiver(
			@Value("${spring.kafka.topicName}") String topic,
			@Value("${spring.kafka.consumer.group-id}") String groupId,
			@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

		Map<String, Object> consumerProps = new HashMap<>();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		ReceiverOptions<String, String> receiverOptions =
				ReceiverOptions.<String, String>create(consumerProps)
						.subscription(Collections.singleton(topic));
		return KafkaReceiver.create(receiverOptions);
	}
}


@Component
@Slf4j
@RequiredArgsConstructor
class MyKafkaReceiver {
	final KafkaReceiver<String, String> kafkaReceiver;

	@Bean
	ApplicationListener<ApplicationReadyEvent> onStart() {
		return event -> {
			Flux<ReceiverRecord<String, String>> inboundFlux = kafkaReceiver.receive();
			inboundFlux
					.subscribe(
						r -> {
							log.info("partition {}, offset: {}, key: {}, message: {}", r.partition(), r.offset(), r.key(), r.value());
							r.receiverOffset().acknowledge();
						},
						e -> {
							log.error("{}",e);
						}
					);
		};
	}
}