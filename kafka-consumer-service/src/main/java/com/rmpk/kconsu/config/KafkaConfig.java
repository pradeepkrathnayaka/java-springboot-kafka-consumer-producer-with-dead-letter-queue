package com.rmpk.kconsu.config;

import com.rmpk.kconsu.exception.NonRetryableException;
import com.rmpk.kconsu.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
@Configuration
public class KafkaConfig {

    // ── Injected so we inherit bootstrap-servers, group-id, etc. from yaml ──
    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${app.kafka.topics.main}")
    private String mainTopic;

    @Value("${app.kafka.topics.dlt}")
    private String dltTopic;

    @Value("${app.kafka.retry.max-attempts}")
    private int maxAttempts;

    @Value("${app.kafka.retry.initial-interval-ms}")
    private long initialInterval;

    @Value("${app.kafka.retry.multiplier}")
    private double multiplier;

    @Value("${app.kafka.retry.max-interval-ms}")
    private long maxInterval;

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ Topic Auto-Creation
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public NewTopic mainTopic() {
        return TopicBuilder.name(mainTopic)
                .partitions(3)
                .replicas(1)                          // ≥3 replicas in production
                .config("min.insync.replicas", "1")
                .build();
    }

    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(dltTopic)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000)) // 30 days
                .config("min.insync.replicas", "1")
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ FIX #1 + #2 — Consumer Factory with ErrorHandlingDeserializer
    //    and cross-service TYPE_MAPPINGS
    //
    //  FIX #1: Wrap JacksonJsonDeserializer with ErrorHandlingDeserializer so
    //          that deserialization failures are routed to the DLT instead of
    //          crashing the listener container.
    //          The raw SerializationException is caught inside
    //          ErrorHandlingDeserializer and re-surfaced as a normal
    //          ListenerExecutionFailedException that DefaultErrorHandler
    //          CAN handle.
    //
    //  FIX #2: Add TYPE_MAPPINGS so the consumer translates the producer's
    //          FQCN (com.rmpk.kprodu.model.OrderEvent) to its own local
    //          class (com.rmpk.kconsu.model.OrderEvent) without needing to
    //          widen TRUSTED_PACKAGES to "*" or sharing the same package.
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());

        // Remove the deserializer class entries — we supply instances directly below
        props.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        props.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        // --- Configure JacksonJsonDeserializer (Jackson 3 — Spring Kafka 4.0+) ---
        JacksonJsonDeserializer<Object> jsonDeserializer = new JacksonJsonDeserializer<>();
        jsonDeserializer.configure(Map.of(
                // Map the producer's FQCN to the local model class (FIX #2)
                JacksonJsonDeserializer.TYPE_MAPPINGS,
                    "com.rmpk.kprodu.model.OrderEvent:com.rmpk.kconsu.model.OrderEvent",
                // Trust only our own model package (least-privilege)
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                    "com.rmpk.kconsu.model",
                // Honour the __TypeId__ header set by the producer
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, "true"
        ), false /* isKey */);

        // --- Wrap both deserializers with ErrorHandlingDeserializer (FIX #1) ------
        ErrorHandlingDeserializer<String> keyDeser =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<Object> valueDeser =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(props, keyDeser, valueDeser);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ FIX #3 — Dedicated String Consumer Factory for MessageConsumer
    //
    //  test-topic carries raw String values with no JSON type headers.
    //  Using the JsonDeserializer for that topic causes:
    //    "No type information in headers and no default type provided"
    //  Solution: a separate factory that uses plain StringDeserializer.
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        // Explicitly override with String deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ FIX #5 — Explicit ProducerFactory + KafkaTemplate
    //
    //  DeadLetterPublishingRecoverer requires a KafkaTemplate<String, Object>.
    //  Without an explicit producer config in yaml, Spring Boot auto-configures
    //  a KafkaTemplate<Object,Object> which creates a type-mismatch at runtime.
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        // Include type headers so downstream consumers can deserialize correctly
        props.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ Dead Letter Publishing Recoverer
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, Object> kafkaTemplate) {

        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver =
                (record, ex) -> {
                    log.error("🔀 Routing to DLT → Topic: {}, Key: {}, Exception: {}",
                            record.topic(), record.key(), ex.getMessage());
                    return new TopicPartition(dltTopic, record.partition());
                };

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate, destinationResolver);
        recoverer.setVerifyPartition(true);
        return recoverer;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ Error Handler with Exponential Backoff + DLT routing
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {

        ExponentialBackOff backOff = new ExponentialBackOff(initialInterval, multiplier);
        backOff.setMaxInterval(maxInterval);
        // Total elapsed time cap: allow all maxAttempts to complete at maxInterval
        backOff.setMaxElapsedTime(maxInterval * maxAttempts);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // ── Non-retryable: bad data, deserialization problems → straight to DLT ─
        errorHandler.addNotRetryableExceptions(
                NonRetryableException.class,
                tools.jackson.core.exc.StreamReadException.class,
                org.apache.kafka.common.errors.SerializationException.class,
                ClassCastException.class,
                IllegalArgumentException.class    // covers trusted-packages failures
        );

        // ── Retryable: transient failures → backoff, then DLT ───────────────────
        errorHandler.addRetryableExceptions(
                RetryableException.class,
                java.net.SocketTimeoutException.class
        );

        // ── Log each retry attempt ───────────────────────────────────────────────
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("🔄 Retry attempt {} for key={} on topic={}: {}",
                        deliveryAttempt, record.key(), record.topic(), ex.getMessage())
        );

        return errorHandler;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ Main Listener Container Factory — JSON / OrderEvent consumers
    //    Used by: OrderConsumer, DltConsumer
    //    Overrides Spring Boot's auto-configured "kafkaListenerContainerFactory"
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        // Single-record acknowledgement (FIX #4 — not batch)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // Micrometer metrics via Observation API
        factory.getContainerProperties().setObservationEnabled(true);

        return factory;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ✅ String Listener Container Factory — plain-text consumers
    //    Used by: MessageConsumer (test-topic, raw String values)
    // ═══════════════════════════════════════════════════════════════════════

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> stringKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(stringConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }
}