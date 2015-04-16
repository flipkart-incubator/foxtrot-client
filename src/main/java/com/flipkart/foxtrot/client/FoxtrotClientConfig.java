package com.flipkart.foxtrot.client;

/**
 * Configuration for the foxtrot client.
 */
public class FoxtrotClientConfig {
    /**
     * The foxtrot table to connect to.
     */
    private String table;

    /**
     * The foxtrot host or load balancer from which cluster member information will be polled.
     */
    private String host;

    /**
     * The port on the host or load balancer from which cluster member information will be polled. (Default: 80)
     */
    private int port = 80;

    /**
     * Maximum number of connections to establish for metadata polling. (Default: 10)
     */
    private int maxConnections = 10;

    /**
     * Cluster metadata polling interval in seconds. (Default: 1 sec)
     */
    private int refreshIntervalSecs = 1;

    /**
     * If {@link com.flipkart.foxtrot.client.senders.QueuedSender} is used, the number of messages to push per batch.
     * (Default: 200)
     */
    private int batchSize = 200;

    /**
     * The file system path where the {@link com.flipkart.foxtrot.client.senders.QueuedSender} will save the events.
     * NOTE: If this is not provided an instance of {@link com.flipkart.foxtrot.client.senders.HttpAsyncEventSender} is used.
     */
    private String localQueuePath;

    public FoxtrotClientConfig() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String appName) {
        this.table = appName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getRefreshIntervalSecs() {
        return refreshIntervalSecs;
    }

    public void setRefreshIntervalSecs(int refreshIntervalSecs) {
        this.refreshIntervalSecs = refreshIntervalSecs;
    }

    public String getLocalQueuePath() {
        return localQueuePath;
    }

    public void setLocalQueuePath(String localQueuePath) {
        this.localQueuePath = localQueuePath;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
