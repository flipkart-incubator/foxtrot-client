package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
import com.flipkart.foxtrot.client.serialization.SerializationException;
import com.google.common.base.Preconditions;
import com.google.common.net.MediaType;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

public class HttpSyncEventSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(HttpSyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private CloseableHttpClient httpClient;

    public HttpSyncEventSender(FoxtrotClientConfig config, FoxtrotCluster client, EventSerializationHandler serializationHandler) {
        super(serializationHandler);
        this.table = config.getTable();
        this.client = client;
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(1024); //Probably max number of foxtrot hosts
        cm.setDefaultMaxPerRoute(1); //Useless to set more per route as only one thread is sending data
        this.httpClient = HttpClients.custom().setConnectionManager(cm).build();
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
        httpClient.close();
        logger.debug("Closed HTTP Client");
    }

    public void send(byte[] payload) {
        FoxtrotClusterMember clusterMember = client.member();
        Preconditions.checkNotNull(clusterMember, "No members found in foxtrot cluster");
        CloseableHttpResponse response = null;
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
            response = httpClient.execute(post);
            if(response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                throw new RuntimeException("Could not send event: " + EntityUtils.toString(response.getEntity()));
            }
            logger.debug("Published event to {}:{}", clusterMember.getHost(), clusterMember.getPort());
        } catch (URISyntaxException | IOException e) {
            logger.error("Unable to publish event to foxtrot", e);
        } finally {
            if(null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
