package com.flipkart.foxtrot.client;

public class FoxtrotClientConfig {
    private String appName;
    private String host;
    private int port;
    private int maxConnections = 10;
    private int refreshInterval = 1;
    private int batchSize = 100;
    private String localQueuePath;

    public FoxtrotClientConfig() {
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
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

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(int refreshInterval) {
        this.refreshInterval = refreshInterval;
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
