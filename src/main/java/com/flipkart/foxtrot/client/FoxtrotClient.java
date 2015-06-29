package com.flipkart.foxtrot.client;

import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.selectors.MemberSelector;
import com.flipkart.foxtrot.client.selectors.RandomSelector;
import com.flipkart.foxtrot.client.senders.HttpAsyncEventSender;
import com.flipkart.foxtrot.client.senders.HttpSyncEventSender;
import com.flipkart.foxtrot.client.senders.QueuedSender;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
import com.flipkart.foxtrot.client.serialization.JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl;
import com.flipkart.foxtrot.client.serialization.JacksonJsonSerializationHandler;
import com.flipkart.foxtrot.client.util.TypeChecker;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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
        Preconditions.checkNotNull(config.getTable());
        Preconditions.checkNotNull(config.getClientType());
        Preconditions.checkNotNull(config.getHost());
        Preconditions.checkArgument(config.getPort() >= 80);
        switch (config.getClientType()) {
            case sync:
                this.eventSender = new HttpSyncEventSender(config, foxtrotCluster, serializationHandler);
                break;
            case async:
                this.eventSender = new HttpAsyncEventSender(config, foxtrotCluster, serializationHandler);
                break;
            case queued:
                List<String> messages = new ArrayList<>();
                if (StringUtils.isEmpty(config.getQueuePath())) {
                    messages.add(String.format("table=%s empty_local_queue_path", config.getTable()));
                }
                if (config.getBatchSize() <= 1) {
                    messages.add(String.format("table=%s invalid_batchSize must_be_greater_than_1", config.getTable()));
                }
                if (!messages.isEmpty()) {
                    throw new Exception(messages.toString());
                }
                this.eventSender = new QueuedSender(new HttpSyncEventSender(config, foxtrotCluster, serializationHandler),
                        serializationHandler,
                        config.getQueuePath(),
                        config.getBatchSize()
                );
                break;
            default:
                throw new Exception(
                        String.format("table=%s invalid_client_type type_provided=%s allowed_types=%s",
                                config.getTable(),
                                config.getClientType(),
                                StringUtils.join(ClientType.values(), ",")
                        )
                );
        }
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
