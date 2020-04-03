package com.flipkart.foxtrot.client.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.client.serialization.DeserializationException;
import com.flipkart.foxtrot.client.serialization.SerDe;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

    private JsonUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static Map<String, Object> readMapFromString(String json) throws DeserializationException {
        return fromJson(json, new TypeReference<Map<String, Object>>() {
        });
    }

    private static <T> T fromJson(String json, TypeReference<T> typeReference) throws DeserializationException {
        try {
            return SerDe.mapper().readValue(json, typeReference);
        } catch (IOException e) {
            log.error("Error while deserializing in fromJson for json : {}, error: {}", json, e);
            throw new DeserializationException("Unable to deserialize data with type reference", e);
        }
    }
}
