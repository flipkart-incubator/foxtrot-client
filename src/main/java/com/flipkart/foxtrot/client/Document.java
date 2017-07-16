package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.hibernate.validator.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class Document {
    @NotNull
    @NotEmpty
    @JsonProperty
    private String id;

    @JsonProperty
    private long timestamp;

    @NotNull
    @JsonProperty
    private Object data;

    public Document() {
        this.timestamp = System.currentTimeMillis();
    }

    public Document(String id, long timestamp, JsonNode data) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
