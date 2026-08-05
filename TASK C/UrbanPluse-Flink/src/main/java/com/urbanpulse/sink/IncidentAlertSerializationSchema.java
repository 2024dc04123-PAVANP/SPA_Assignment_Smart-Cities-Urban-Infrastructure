package com.urbanpulse.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.urbanpulse.model.IncidentAlert;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;

public class IncidentAlertSerializationSchema
        implements SerializationSchema<IncidentAlert> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(IncidentAlert alert) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            return objectMapper.writeValueAsBytes(alert);

        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to serialize Incident Alert",
                    exception
            );
        }
    }
}