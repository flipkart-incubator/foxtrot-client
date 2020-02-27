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
import com.google.common.collect.Lists;
import com.squareup.okhttp.ConnectionPool;
import feign.Feign;
import feign.FeignException;
import feign.Response;
import feign.okhttp.OkHttpClient;
import feign.slf4j.Slf4jLogger;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class HttpSyncEventSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(HttpSyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private FoxtrotHttpClient httpClient;

    private final static Slf4jLogger slf4jLogger = new Slf4jLogger();

    private static List<String> ignoreableFailureReasons = Lists.newArrayList();


    public HttpSyncEventSender(final FoxtrotClientConfig config, FoxtrotCluster client, EventSerializationHandler serializationHandler) {
        super(serializationHandler);
        this.table = config.getTable();
        this.client = client;
        com.squareup.okhttp.OkHttpClient okHttpClient = new com.squareup.okhttp.OkHttpClient();
        okHttpClient.setConnectionPool(new ConnectionPool(config.getMaxConnections(), config.getKeepAliveTimeMillis()));
        this.httpClient = Feign.builder()
                .client(new OkHttpClient(okHttpClient))
                .logger(slf4jLogger)
                .logLevel(feign.Logger.Level.BASIC)
                .target(new FoxtrotTarget<>(FoxtrotHttpClient.class, "foxtrot", client));
        ignoreableFailureReasons = Arrays.asList(config.getCommaSeparatedIgnorableFailureMessages().split(","));
    }

    @Override
    public void send(Document document) {
        send(table, document);
    }

    @Override
    public void send(String table, Document document) {
        send(table, Collections.singletonList(document));
    }

    @Override
    public void send(List<Document> documents) {
        send(table, documents);
    }

    @Override
    public void send(String table, List<Document> documents) {
        try {
            send(table, getSerializationHandler().serialize(documents));
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {

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
            } else if (response.status() == 500){
                final boolean[] ignoreException = {false};
                //TODO Based on reason we set in foxtrot server. Need to add mapping parsing exception
                ignoreableFailureReasons.forEach(s -> {
                    if (response.reason().contains(s)){
                        ignoreException[0] = true;
                    }
                });
                if (!ignoreException[0]){
                    throw new RuntimeException(String.format("table=%s event_send_failed status [%d] exception_message=%s", table, response.status(), response.reason()));
                }
            }else {
                throw new RuntimeException(String.format("table=%s event_send_failed status [%d] exception_message=%s", table, response.status(), response.reason()));
            }
        } catch (FeignException e) {
            logger.error("table={} msg=event_publish_failed", new Object[]{table}, e);
            throw new RuntimeException("msg=event_publish_failed with exception : " , e);
        }
    }

    private boolean is2XX(int status) {
        return status / 100 == 2;
    }
}
