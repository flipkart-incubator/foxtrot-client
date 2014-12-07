package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
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
    private static final ObjectMapper mapper;
    private final CloseableHttpClient httpClient;

    private AtomicReference<FoxtrotClusterStatus> status;
    private final URI uri;

    static {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private ClusterStatusUpdater(AtomicReference<FoxtrotClusterStatus> status, CloseableHttpClient httpClient, URI uri) {
        this.status = status;
        this.httpClient = httpClient;
        this.uri = uri;
    }

    public static ClusterStatusUpdater create(FoxtrotClientConfig config, AtomicReference<FoxtrotClusterStatus> status) throws Exception {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxPerRoute(
                new HttpRoute(new HttpHost(config.getHost(), config.getPort())), config.getMaxConnections());
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();

        return new ClusterStatusUpdater(status, httpClient,
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
            logger.info("Initiating data get");
            HttpGet httpGet = new HttpGet(uri);
            response = httpClient.execute(httpGet);
            if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                logger.error("Error getting status: ", response.getStatusLine().getReasonPhrase());
            }
            HttpEntity entity = response.getEntity();
            FoxtrotClusterStatus foxtrotClusterStatus = mapper.readValue(EntityUtils.toByteArray(entity), FoxtrotClusterStatus.class);
            status.set(foxtrotClusterStatus);
        } catch (IOException e) {
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
