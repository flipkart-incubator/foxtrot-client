package com.flipkart.foxtrot.client.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterStatus;

import java.io.IOException;

public class JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl implements FoxtrotClusterResponseSerializationHandler {

    private JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl() {
    }

    @Override
    public FoxtrotClusterStatus deserialize(byte[] data) throws DeserializationException {
        try {
            return SerDe.mapper().readValue(data, FoxtrotClusterStatus.class);
        } catch (IOException e) {
            throw new DeserializationException("Could not deserialize foxtrot response: " + e.getLocalizedMessage(), e);
        }
    }
}
