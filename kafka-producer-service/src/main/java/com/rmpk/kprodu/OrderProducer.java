package com.rmpk.kprodu;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.rmpk.kprodu.model.OrderEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

	private final KafkaTemplate<String, Object> kafkaTemplate;

	@Value("${app.kafka.topics.main}")
	private String mainTopic;

	public CompletableFuture<SendResult<String, Object>> sendOrder(OrderEvent order) {
		log.info("📤 Producing order: {}", order.getOrderId());

		return kafkaTemplate.send(mainTopic, order.getOrderId(), order).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("❌ Failed to send order {}: {}", order.getOrderId(), ex.getMessage());
			} else {
				log.info("✅ Order {} → partition={}, offset={}", order.getOrderId(),
						result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
			}
		});
	}
}