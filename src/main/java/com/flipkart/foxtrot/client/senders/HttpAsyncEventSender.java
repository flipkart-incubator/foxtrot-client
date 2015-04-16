package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.EventSerializationHandler;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.serialization.SerializationException;
import com.google.common.base.Preconditions;
import com.google.common.net.MediaType;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpAsyncEventSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(HttpAsyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private final ScheduledExecutorService executorService;
    private CloseableHttpAsyncClient httpClient;

    public HttpAsyncEventSender(FoxtrotClientConfig config, FoxtrotCluster client, EventSerializationHandler serializationHandler) throws IOReactorException {
        super(serializationHandler);
        this.table = config.getTable();
        this.client = client;
        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
        PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
        cm.setMaxTotal(100); //Probably max number of foxtrot hosts
        this.httpClient = HttpAsyncClients.custom().setConnectionManager(cm).build();
        Evictor connEvictor = new Evictor(cm);
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleWithFixedDelay(connEvictor, 1, 5, TimeUnit.SECONDS);
        httpClient.start();
    }

    @Override
    public void send(Document document) {
        send(Collections.singletonList(document));
    }

    @Override
    public void send(List<Document> documents) {
        try {
            send(getSerializationHandler().serialize(documents));
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        executorService.shutdownNow();
        logger.debug("Shut down connection evictor");
        httpClient.close();
        logger.debug("Closed HTTP Client");
    }

    public void send(byte[] payload) {
        FoxtrotClusterMember clusterMember = client.member();
        Preconditions.checkNotNull(clusterMember, "No members found in foxtrot cluster");
        try {
            URI requestURI = new URIBuilder()
                    .setScheme("http")
                    .setHost(clusterMember.getHost())
                    .setPort(clusterMember.getPort())
                    .setPath(String.format("/foxtrot/v1/document/%s/bulk", table))
                    .build();
            HttpPost post = new HttpPost(requestURI);
            post.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
            post.setEntity(new ByteArrayEntity(payload));
            httpClient.execute(post, new FutureCallback<HttpResponse>() {

                public void completed(final HttpResponse response) {
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                        try {
                            logger.error("Could not send event: {}", EntityUtils.toString(response.getEntity()));
                        } catch (IOException e) {
                            logger.error("Could not deserialize API response.", e);
                        }
                    }
                }

                public void failed(final Exception ex) {
                    logger.error("Could not send event.", ex);
                }

                public void cancelled() {
                    logger.error("Call to foxtrot cancelled.");
                }

            });
            logger.debug("Sent event to {}:{}", clusterMember.getHost(), clusterMember.getPort());
        } catch (URISyntaxException e) {
            logger.error("Invalid syntax: ", e);
        }

    }

    private static class Evictor implements Runnable {

        private final NHttpClientConnectionManager connectionManager;

        public Evictor(NHttpClientConnectionManager connectionManager) {
            super();
            this.connectionManager = connectionManager;
        }

        @Override
        public void run() {
            try {
                connectionManager.closeExpiredConnections();
                connectionManager.closeIdleConnections(5, TimeUnit.SECONDS);
            } catch (Exception ex) {
                logger.error("Error cleaning up connections.", ex);
            }
        }

    }
}
