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
     * Type of client which will init. Can be any of {@link com.flipkart.foxtrot.client.ClientType}
     * Default value is {@link com.flipkart.foxtrot.client.ClientType#async}
     */
    private ClientType clientType = ClientType.async;

    /**
     * Used if clientType is {@link com.flipkart.foxtrot.client.ClientType#queued}
     * or {@link com.flipkart.foxtrot.client.ClientType#queued}
     * Temporary file system path where events will be saved before ingestion.
     */
    private String queuePath;

    /**
     * Used if clientType is {@link com.flipkart.foxtrot.client.ClientType#queued}
     * or {@link com.flipkart.foxtrot.client.ClientType#queued}
     * Number of messages to push per batch.
     * (Default: 200)
     */
    private int batchSize = 200;

    public FoxtrotClientConfig() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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

    public ClientType getClientType() {
        return clientType;
    }

    public void setClientType(ClientType clientType) {
        this.clientType = clientType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getQueuePath() {
        return queuePath;
    }

    public void setQueuePath(String queuePath) {
        this.queuePath = queuePath;
    }
}
