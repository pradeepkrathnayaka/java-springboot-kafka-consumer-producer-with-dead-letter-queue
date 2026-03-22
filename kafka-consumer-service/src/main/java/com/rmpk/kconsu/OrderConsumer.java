package com.rmpk.kconsu;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.rmpk.kconsu.exception.NonRetryableException;
import com.rmpk.kconsu.exception.RetryableException;
import com.rmpk.kconsu.model.OrderEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

	private final OrderProcessingService orderProcessingService;

	@KafkaListener(topics = "${app.kafka.topics.main}", groupId = "order-consumer-group", containerFactory = "kafkaListenerContainerFactory")
	public void consume(@Payload OrderEvent order, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset,
			@Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

		log.info("📥 Received [key={}, topic={}, partition={}, offset={}]: {}", key, topic, partition, offset, order);

		try {
			validateOrder(order);
			orderProcessingService.processOrder(order);
			log.info("✅ Processed order: {}", order.getOrderId());

		} catch (NonRetryableException ex) {
			log.error("❌ Non-retryable for order {}: {}", order.getOrderId(), ex.getMessage());
			throw ex; // → immediately sent to DLT
		} catch (RetryableException ex) {
			log.warn("⚠️ Retryable for order {}: {}", order.getOrderId(), ex.getMessage());
			throw ex; // → retried with backoff, then DLT
		}
	}

	private void validateOrder(OrderEvent order) {
		if (order.getOrderId() == null || order.getOrderId().isBlank()) {
			throw new NonRetryableException("Order ID is missing");
		}
		if (order.getQuantity() <= 0) {
			throw new NonRetryableException("Invalid quantity: " + order.getQuantity());
		}
	}
}