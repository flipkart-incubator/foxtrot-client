package com.flipkart.foxtrot.client.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterStatus;

import java.io.IOException;

public class JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl implements FoxtrotClusterResponseSerializationHandler {
    private final ObjectMapper mapper;
    public static final FoxtrotClusterResponseSerializationHandler INSTANCE = new JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl();

    private JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl() {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    @Override
    public FoxtrotClusterStatus deserialize(byte[] data) throws DeserializatiionException {
        try {
            return mapper.readValue(data, FoxtrotClusterStatus.class);
        } catch (IOException e) {
            throw new DeserializatiionException("Could not deserialize foxtrot response: " + e.getLocalizedMessage(), e);
        }
    }
}
