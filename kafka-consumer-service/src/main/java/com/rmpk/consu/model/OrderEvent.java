package com.rmpk.consu.model;

import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
	private String orderId;
	private String customerId;
	private String product;
	private int quantity;
	private BigDecimal totalAmount;
	private OrderStatus status;
	private Instant createdAt;

	public enum OrderStatus {
		PENDING, PROCESSING, COMPLETED, FAILED
	}
}
