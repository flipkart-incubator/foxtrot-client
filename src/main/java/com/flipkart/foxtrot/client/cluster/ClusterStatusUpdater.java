package com.flipkart.foxtrot.client.cluster;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.serialization.FoxtrotClusterResponseSerializationHandler;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterStatusUpdater implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClusterStatusUpdater.class.getSimpleName());
    private final CloseableHttpClient httpClient;

    private AtomicReference<FoxtrotClusterStatus> status;
    private final URI uri;
    private final FoxtrotClusterResponseSerializationHandler serializationHandler;

    private ClusterStatusUpdater(AtomicReference<FoxtrotClusterStatus> status, CloseableHttpClient httpClient,
                                 FoxtrotClusterResponseSerializationHandler serializationHandler,
                                 URI uri) {
        this.status = status;
        this.httpClient = httpClient;
        this.uri = uri;
        this.serializationHandler = serializationHandler;
    }

    public static ClusterStatusUpdater create(FoxtrotClientConfig config,
                                              AtomicReference<FoxtrotClusterStatus> status,
                                              FoxtrotClusterResponseSerializationHandler serializationHandler) throws Exception {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxPerRoute(
                new HttpRoute(new HttpHost(config.getHost(), config.getPort())), config.getMaxConnections());
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();

        return new ClusterStatusUpdater(status, httpClient, serializationHandler,
                new URIBuilder().setHost(config.getHost())
                        .setPort(config.getPort())
                        .setPath("/foxtrot/v1/cluster/members")
                        .setScheme("http")
                        .build());
    }

    @Override
    public void run() {
        CloseableHttpResponse response = null;
        try {
            logger.debug("Initiating data get");
            HttpGet httpGet = new HttpGet(uri);
            response = httpClient.execute(httpGet);
            if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                logger.error("Error getting status: ", response.getStatusLine().getReasonPhrase());
                return;
            }
            HttpEntity entity = response.getEntity();
            final byte data[] =EntityUtils.toByteArray(entity);
            logger.trace("Received data: {}", new String(data));
            FoxtrotClusterStatus foxtrotClusterStatus = serializationHandler.deserialize(data);
            status.set(foxtrotClusterStatus);
        } catch (Exception e) {
            logger.error("Error getting cluster data: ", e);
        } finally {
            if(null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    logger.error("Error closing connection: ", e);
                }
            }
        }
    }
}
