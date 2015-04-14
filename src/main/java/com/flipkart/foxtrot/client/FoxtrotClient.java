package com.flipkart.foxtrot.client;

import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.selectors.RandomSelector;
import com.flipkart.foxtrot.client.senders.HttpAsyncEventSender;
import com.flipkart.foxtrot.client.senders.HttpSyncEventSender;
import com.flipkart.foxtrot.client.senders.QueuedSender;
import com.flipkart.foxtrot.client.serialization.JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl;
import com.flipkart.foxtrot.client.serialization.JacksonJsonSerializationHandler;
import com.flipkart.foxtrot.client.util.TypeChecker;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;

public class FoxtrotClient {
    private final FoxtrotCluster foxtrotCluster;
    private final EventSender eventSender;

    public FoxtrotClient(FoxtrotClientConfig config) throws Exception {
        this(config, new RandomSelector(), JacksonJsonSerializationHandler.INSTANCE);
    }

    public FoxtrotClient(FoxtrotClientConfig config,
                         MemberSelector memberSelector,
                         EventSerializationHandler serializationHandler) throws Exception {
        this.foxtrotCluster = new FoxtrotCluster(config, memberSelector,
                                                    JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl.INSTANCE);
        this.eventSender
                = Strings.isNullOrEmpty(config.getLocalQueuePath())
                    ? new HttpAsyncEventSender(config, foxtrotCluster, serializationHandler)
                    : new QueuedSender(
                            new HttpSyncEventSender(config, foxtrotCluster, serializationHandler),
                            serializationHandler,
                            config.getLocalQueuePath(),
                            config.getBatchSize(),
                            config.getRefreshIntervalSecs());
    }

    public FoxtrotClient(FoxtrotCluster foxtrotCluster, EventSender eventSender) {
        this.foxtrotCluster = foxtrotCluster;
        this.eventSender = eventSender;
    }

    public void send(Document document) throws Exception {
        Preconditions.checkNotNull(document.getData());
        Preconditions.checkArgument(!TypeChecker.isPrimitive(document.getData()));
        eventSender.send(document);
    }

    public void send(List<Document> documents) throws Exception {
        eventSender.send(documents);
    }

    public void close() throws Exception {
        eventSender.close();
        foxtrotCluster.stop();
    }
}
