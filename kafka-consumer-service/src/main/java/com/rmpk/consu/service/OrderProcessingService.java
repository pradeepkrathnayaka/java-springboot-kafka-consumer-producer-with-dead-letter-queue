package com.rmpk.consu.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import com.rmpk.consu.model.OrderEvent;
import com.rmpk.kconsu.exception.NonRetryableException;
import com.rmpk.kconsu.exception.RetryableException;

import java.math.BigDecimal;

@Slf4j
@Service
public class OrderProcessingService {

	public void processOrder(OrderEvent order) {
		log.info("⚙️ Processing order: {}", order.getOrderId());

		// 🔴 Non-retryable: bad data → straight to DLT
		if (order.getTotalAmount() != null && order.getTotalAmount().compareTo(BigDecimal.ZERO) < 0) {
			throw new NonRetryableException("Negative total amount: " + order.getOrderId());
		}

		// 🟡 Retryable: simulated transient failure
		if ("SIMULATE_TRANSIENT_FAIL".equals(order.getProduct())) {
			throw new RetryableException("Downstream service timeout: " + order.getOrderId());
		}

		log.info("✅ Order {} completed!", order.getOrderId());
	}
}
