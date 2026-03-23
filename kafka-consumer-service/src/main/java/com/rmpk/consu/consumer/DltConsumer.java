package com.rmpk.consu.consumer;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DltConsumer {

    /**
     * ✅ Explicit containerFactory added.
     *
     * Without it, Spring falls back to the auto-configured factory which
     * inherits listener.type from application.yaml.  Being explicit ensures
     * this consumer always uses our ErrorHandlingDeserializer-backed factory
     * regardless of any yaml-level listener.type changes in the future.
     *
     * DLT messages are JSON (originally from orders-topic), so
     * kafkaListenerContainerFactory (JsonDeserializer) is the correct choice.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.dlt}",
            groupId = "dlt-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlt(ConsumerRecord<String, Object> record) {

        String originalTopic   = extractHeader(record, "kafka_dlt-original-topic");
        String originalOffset  = extractHeader(record, "kafka_dlt-original-offset");
        String exceptionFqcn   = extractHeader(record, "kafka_dlt-exception-fqcn");
        String exceptionMsg    = extractHeader(record, "kafka_dlt-exception-message");

        log.error("☠️ ════════════════════════════════════════════");
        log.error("☠️  DEAD LETTER RECEIVED");
        log.error("☠️  Key:               {}", record.key());
        log.error("☠️  Value:             {}", record.value());
        log.error("☠️  DLT Topic:         {}", record.topic());
        log.error("☠️  DLT Partition:     {}", record.partition());
        log.error("☠️  DLT Offset:        {}", record.offset());
        log.error("☠️  Original Topic:    {}", originalTopic);
        log.error("☠️  Original Offset:   {}", originalOffset);
        log.error("☠️  Exception Type:    {}", exceptionFqcn);
        log.error("☠️  Exception Message: {}", exceptionMsg);
        log.error("☠️ ════════════════════════════════════════════");

        // ✅ Best Practice: Persist for manual re-processing
        // failedMessageRepository.save(FailedMessage.from(record));

        // ✅ Best Practice: Alert (Slack, PagerDuty, email)
        // alertService.sendDltAlert(record, exceptionMsg);
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerKey) {
        return Optional.ofNullable(record.headers().lastHeader(headerKey))
                .map(h -> new String(h.value(), StandardCharsets.UTF_8))
                .orElse("N/A");
    }
}