package com.flipkart.foxtrot.client;

import java.util.List;

public abstract class EventSender {
    private final EventSerializationHandler serializationHandler;

    public EventSender(EventSerializationHandler serializationHandler) {
        this.serializationHandler = serializationHandler;
    }

    abstract public void send(Document document) throws Exception;
    abstract public void send(List<Document> documents) throws Exception;

    abstract public void close() throws Exception;

    protected EventSerializationHandler getSerializationHandler() {
        return serializationHandler;
    }
}
