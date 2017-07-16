package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.selectors.FoxtrotTarget;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
import com.flipkart.foxtrot.client.serialization.SerializationException;
import com.google.common.base.Preconditions;
import feign.Feign;
import feign.FeignException;
import feign.Response;
import feign.okhttp.OkHttpClient;
import feign.slf4j.Slf4jLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpSyncEventSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(HttpSyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private FoxtrotHttpClient httpClient;

    private final static Slf4jLogger slf4jLogger = new Slf4jLogger();


    public HttpSyncEventSender(final FoxtrotClientConfig config, FoxtrotCluster client, EventSerializationHandler serializationHandler) {
        super(serializationHandler);
        this.table = config.getTable();
        this.client = client;
        okhttp3.OkHttpClient.Builder okHttpClient = new okhttp3.OkHttpClient.Builder();
        okHttpClient.connectionPool(new okhttp3.ConnectionPool(config.getMaxConnections(), config.getKeepAliveTimeMillis(), TimeUnit.MILLISECONDS));
        this.httpClient = Feign.builder()
                .client(new OkHttpClient(okHttpClient.build()))
                .logger(slf4jLogger)
                .logLevel(feign.Logger.Level.BASIC)
                .target(new FoxtrotTarget<>(FoxtrotHttpClient.class, "foxtrot", client));
    }

    @Override
    public void send(Document document) throws Exception {
        send(table, document);
    }

    @Override
    public void send(String table, Document document) throws Exception {
        send(table, Collections.singletonList(document));
    }

    @Override
    public void send(List<Document> documents) throws Exception {
        send(table, documents);
    }

    @Override
    public void send(String table, List<Document> documents) throws Exception {
        try {
            send(table, getSerializationHandler().serialize(documents));
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() throws Exception {

    }

    public void send(final String table, byte[] payload) {
        FoxtrotClusterMember clusterMember = client.member();
        Preconditions.checkNotNull(clusterMember, "No members found in foxtrot cluster");
        try {
            Response response = httpClient.send(table, payload);
            if (is2XX(response.status())) {
                logger.info("table={} messages_sent host={} port={}", table, clusterMember.getHost(), clusterMember.getPort());
            } else if (response.status() == 400) {
                logger.error("table={} host={} port={} statusCode={}", table, clusterMember.getHost(), clusterMember.getPort(), response.status());
            } else {
                throw new RuntimeException(String.format("table=%s event_send_failed status [%d] exception_message=%s", table, response.status(), response.reason()));
            }
        } catch (FeignException e) {
            logger.error("table={} msg=event_publish_failed", new Object[]{table}, e);
        }
    }

    private boolean is5XX(int status) {
        return status / 100 == 5;
    }

    private boolean is4XX(int status) {
        return status / 100 == 4;
    }

    private boolean is2XX(int status) {
        return status / 100 == 2;
    }
}
