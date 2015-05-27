package com.flipkart.foxtrot.client.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.Document;

import java.io.IOException;
import java.util.List;

public class JacksonJsonSerializationHandler implements EventSerializationHandler {

    public static final JacksonJsonSerializationHandler INSTANCE = new JacksonJsonSerializationHandler();
    private final ObjectMapper mapper;

    private JacksonJsonSerializationHandler() {
        this.mapper = new ObjectMapper();
        this.mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    @Override
    public byte[] serialize(Document document) throws SerializationException {
        try {
            return mapper.writeValueAsBytes(document);
        } catch (Exception e) {
            throw new SerializationException("Error serializing document: " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public byte[] serialize(List<Document> documents) throws SerializationException {
        try {
            return mapper.writeValueAsBytes(documents);
        } catch (Exception e) {
            throw new SerializationException("Error serializing document: " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public Document deserialize(byte[] data) throws DeserializationException {
        try {
            return mapper.readValue(data, Document.class);
        } catch (IOException e) {
            throw new DeserializationException("Error deserializing document: " + e.getLocalizedMessage(), e);
        }
    }
}
