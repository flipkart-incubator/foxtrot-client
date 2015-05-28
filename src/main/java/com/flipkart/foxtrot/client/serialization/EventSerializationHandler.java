package com.flipkart.foxtrot.client.serialization;

import com.flipkart.foxtrot.client.Document;

import java.util.List;

public interface EventSerializationHandler {
    public byte[] serialize(Document document) throws SerializationException;

    public byte[] serialize(List<Document> documents) throws SerializationException;

    public Document deserialize(byte[] data) throws DeserializationException;
}
