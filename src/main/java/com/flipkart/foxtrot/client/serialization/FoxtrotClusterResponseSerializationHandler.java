package com.flipkart.foxtrot.client.serialization;

import com.flipkart.foxtrot.client.cluster.FoxtrotClusterStatus;

public interface FoxtrotClusterResponseSerializationHandler {
    FoxtrotClusterStatus deserialize(byte[] data) throws DeserializationException;
}
