package com.flipkart.foxtrot.client.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.Document;

import java.io.IOException;
import java.util.List;

public class JacksonJsonSerializationHandler implements EventSerializationHandler {

    public static final JacksonJsonSerializationHandler INSTANCE = new JacksonJsonSerializationHandler();

    private JacksonJsonSerializationHandler() {
    }

    @Override
    public byte[] serialize(Document document) throws SerializationException {
        try {
            return SerDe.mapper().writeValueAsBytes(document);
        } catch (Exception e) {
            throw new SerializationException("Error serializing document: " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public byte[] serialize(List<Document> documents) throws SerializationException {
        try {
            return SerDe.mapper().writeValueAsBytes(documents);
        } catch (Exception e) {
            throw new SerializationException("Error serializing document: " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public Document deserialize(byte[] data) throws DeserializationException {
        try {
            return SerDe.mapper().readValue(data, Document.class);
        } catch (IOException e) {
            throw new DeserializationException("Error deserializing document: " + e.getLocalizedMessage(), e);
        }
    }
}
