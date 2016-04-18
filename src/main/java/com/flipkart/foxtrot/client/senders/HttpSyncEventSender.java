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
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.okhttp.OkHttpClient;
import feign.slf4j.Slf4jLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class HttpSyncEventSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(HttpSyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private FoxtrotHttpClient httpClient;

    private final static JacksonDecoder decoder = new JacksonDecoder();
    private final static JacksonEncoder encoder = new JacksonEncoder();
    private final static Slf4jLogger slf4jLogger = new Slf4jLogger();


    public HttpSyncEventSender(final FoxtrotClientConfig config, FoxtrotCluster client, EventSerializationHandler serializationHandler) {
        super(serializationHandler);
        this.table = config.getTable();
        this.client = client;
        this.httpClient = Feign.builder()
                .decoder(decoder)
                .encoder(encoder)
                .client(new OkHttpClient())
                .logger(slf4jLogger)
                .logLevel(feign.Logger.Level.BASIC)
                .target(new FoxtrotTarget<FoxtrotHttpClient>(FoxtrotHttpClient.class, "foxtrot", client));
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

    }

    public void send(byte[] payload) {
        FoxtrotClusterMember clusterMember = client.member();
        Preconditions.checkNotNull(clusterMember, "No members found in foxtrot cluster");
        try {
            Response response = httpClient.send(table, payload);
            if (response.status() != 200 && response.status() != 201 && response.status() != 202) {
                throw new RuntimeException(String.format("table=%s event_send_failed exception_message=%s", table, response.reason()));
            }
            logger.info("table={} messages_sent host={} port={}", table, clusterMember.getHost(), clusterMember.getPort());
        } catch (FeignException e) {
            logger.error("table={} event_publish_failed", new Object[]{table}, e);
        }
    }
}
