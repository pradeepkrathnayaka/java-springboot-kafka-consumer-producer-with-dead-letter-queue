package com.rmpk.kprodu;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rmpk.kprodu.model.OrderEvent;
import com.rmpk.kprodu.model.OrderEvent.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

	private final OrderProducer orderProducer;

//	curl --location --request POST 'http://localhost:8080/api/orders/success'

	@PostMapping("/success")
	public ResponseEntity<Map<String, String>> sendSuccessOrder() {
		OrderEvent order = OrderEvent.builder().orderId(UUID.randomUUID().toString()).customerId("CUST-001")
				.product("Laptop").quantity(2).totalAmount(new BigDecimal("1999.99")).status(OrderStatus.PENDING)
				.createdAt(Instant.now()).build();
		orderProducer.sendOrder(order);
		return ResponseEntity.ok(Map.of("status", "SENT", "orderId", order.getOrderId()));
	}

//	curl --location --request POST 'http://localhost:8080/api/orders/fail-permanent'

	@PostMapping("/fail-permanent")
	public ResponseEntity<Map<String, String>> sendBadOrder() {
		OrderEvent order = OrderEvent.builder().orderId(UUID.randomUUID().toString()).customerId("CUST-002")
				.product("Phone").quantity(1).totalAmount(new BigDecimal("-100.00")) // ❌ invalid
				.status(OrderStatus.PENDING).createdAt(Instant.now()).build();
		orderProducer.sendOrder(order);
		return ResponseEntity.ok(Map.of("status", "SENT", "orderId", order.getOrderId()));
	}

//	curl --location --request POST 'http://localhost:8080/api/orders/fail-transient'

	@PostMapping("/fail-transient")
	public ResponseEntity<Map<String, String>> sendTransientFailOrder() {
		OrderEvent order = OrderEvent.builder().orderId(UUID.randomUUID().toString()).customerId("CUST-003")
				.product("SIMULATE_TRANSIENT_FAIL").quantity(1).totalAmount(new BigDecimal("49.99"))
				.status(OrderStatus.PENDING).createdAt(Instant.now()).build();
		orderProducer.sendOrder(order);
		return ResponseEntity.ok(Map.of("status", "SENT", "orderId", order.getOrderId()));
	}
}