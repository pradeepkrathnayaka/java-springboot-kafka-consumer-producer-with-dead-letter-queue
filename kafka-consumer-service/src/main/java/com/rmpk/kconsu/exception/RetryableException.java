package com.rmpk.kconsu.exception;

/**
 * ✅ Best Practice: Separate retryable vs non-retryable exceptions.
 * Non-retryable → straight to DLT (bad data, validation errors) Retryable →
 * exponential backoff retries, then DLT (transient failures)
 */
public class RetryableException extends RuntimeException {
	public RetryableException(String message) {
		super(message);
	}

	public RetryableException(String message, Throwable cause) {
		super(message, cause);
	}
}