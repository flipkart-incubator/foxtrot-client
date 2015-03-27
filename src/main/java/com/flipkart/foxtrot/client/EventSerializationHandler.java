package com.flipkart.foxtrot.client;

import com.flipkart.foxtrot.client.serialization.DeserializatiionException;
import com.flipkart.foxtrot.client.serialization.SerializationException;

import java.util.List;

public interface EventSerializationHandler {
    public byte[] serialize(Document document) throws SerializationException;
    public byte[] serialize(List<Document> documents) throws SerializationException;
    public Document deserialize(byte[] data) throws DeserializatiionException;
}
