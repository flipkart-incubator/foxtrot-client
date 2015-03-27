package com.flipkart.foxtrot.client;

import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.selectors.RandomSelector;
import com.flipkart.foxtrot.client.senders.HttpSyncEventSender;
import com.flipkart.foxtrot.client.senders.QueuedSender;
import com.google.common.base.Strings;

import java.util.List;

public class FoxtrotClient {
    private final FoxtrotCluster foxtrotCluster;
    private final EventSender eventSender;

    public FoxtrotClient(FoxtrotClientConfig config) throws Exception {
        this.foxtrotCluster = new FoxtrotCluster(config);
        this.eventSender = (Strings.isNullOrEmpty(config.getLocalQueuePath())
                                ? new HttpSyncEventSender(config.getAppName(), foxtrotCluster)
                                : new QueuedSender(config.getAppName(), foxtrotCluster,
                                                    config.getLocalQueuePath(), config.getBatchSize()));
    }

    public FoxtrotClient(FoxtrotClientConfig config, EventSender eventSender) throws Exception {
        this.foxtrotCluster = new FoxtrotCluster(config, new RandomSelector());
        this.eventSender = eventSender;
    }

    public void send(Document document) throws Exception {
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
