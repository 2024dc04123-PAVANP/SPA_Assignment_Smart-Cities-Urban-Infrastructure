package com.urbanpulse.sink;

/**
 *
 * urbanpulse.incidents topic
 */

import com.urbanpulse.model.IncidentAlert;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class KafkaIncidentSink {

    public static KafkaSink<IncidentAlert> create() {

        return KafkaSink.<IncidentAlert>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<IncidentAlert>builder()
                                .setTopic("urbanpulse.incidents")
                                .setValueSerializationSchema(
                                        new IncidentAlertSerializationSchema()
                                )
                                .build()
                )
                .setDeliveryGuarantee(
                        DeliveryGuarantee.AT_LEAST_ONCE
                )
                .build();
    }
}