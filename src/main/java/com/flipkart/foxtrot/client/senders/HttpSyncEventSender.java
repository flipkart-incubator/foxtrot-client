package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.selectors.FoxtrotTarget;
import com.flipkart.foxtrot.client.serialization.DeserializationException;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
import com.flipkart.foxtrot.client.serialization.SerializationException;
import com.flipkart.foxtrot.client.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.squareup.okhttp.ConnectionPool;
import feign.Feign;
import feign.FeignException;
import feign.Response;
import feign.okhttp.OkHttpClient;
import feign.slf4j.Slf4jLogger;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.ws.rs.core.Response.Status.Family;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSyncEventSender extends EventSender {

    private static final Logger logger = LoggerFactory.getLogger(HttpSyncEventSender.class.getSimpleName());

    private final String table;
    private final FoxtrotCluster client;
    private FoxtrotHttpClient httpClient;

    private final static Slf4jLogger slf4jLogger = new Slf4jLogger();

    private static final String ERROR_MESSAGE = "message";
    private static final String INTERNAL_SERVER_ERROR = "500 INTERNAL SERVER ERROR";

    private static List<String> ignoreableFailureReasons = Lists.newArrayList();


    public HttpSyncEventSender(final FoxtrotClientConfig config, FoxtrotCluster client,
            EventSerializationHandler serializationHandler) {
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

        ignoreableFailureReasons = config.getIgnorableFailureMessagePatterns();
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
            String responseBody = Objects.nonNull(response.body())
                    ? IOUtils.toString(response.body().asInputStream())
                    : "{}";

            if (Family.SUCCESSFUL.equals(Family.familyOf(response.status()))) {
                logger.info("table={} messages_sent host={} port={} response={}", table, clusterMember.getHost(),
                        clusterMember.getPort(), responseBody);
            } else if (response.status() == 400) {
                logger.error("table={} client_error host={} port={} statusCode={} reason={} response={}", table,
                        clusterMember.getHost(), clusterMember.getPort(), response.status(), response.reason(),
                        responseBody);
            } else if (response.status() == 500) {
                logger.debug("table={} server_error host={} port={} statusCode={} reason={} response={}", table,
                        clusterMember.getHost(), clusterMember.getPort(), response.status(), response.reason(),
                        responseBody);

                Map<String, Object> responseMap = JsonUtils.readMapFromString(responseBody);

                Optional<String> throwableFailure = Optional.of(INTERNAL_SERVER_ERROR);
                if (responseMap.containsKey(ERROR_MESSAGE)
                        && responseMap.get(ERROR_MESSAGE) instanceof String
                        && Strings.isNotBlank((String) responseMap.get(ERROR_MESSAGE))) {
                    String[] failureReasons = ((String) responseMap.get(ERROR_MESSAGE))
                            .split(ElasticsearchQueryStore.ERROR_DELIMITER);
                    //TODO: Based on message we set in foxtrot server. Need to add mapping parsing exception

                    throwableFailure = Stream.of(failureReasons)
                            .filter(failureReason ->
                                    ignoreableFailureReasons.stream()
                                            .noneMatch(ignorableFailureReason -> isMatching(failureReason,
                                                    ignorableFailureReason)))
                            .findAny();
                }

                //This is done in case there is even 1 exception which needs retry
                if (throwableFailure.isPresent()) {
                    logger.error(
                            "table={} event_send_failed  host={} port={} statusCode={} reason={} response={} exception_message={}",
                            table, clusterMember.getHost(), clusterMember.getPort(), response.status(),
                            response.reason(), responseBody throwableFailure.get());
                    throw new RuntimeException(
                            String.format("table=%s event_send_failed status [%d] exception_message=%s", table,
                                    response.status(), throwableFailure.get()));
                }
            } else {
                throw new RuntimeException(
                        String.format("table=%s event_send_failed status [%d] exception_message=%s", table,
                                response.status(), response.reason()));
            }
        } catch (FeignException | IOException | DeserializationException e) {
            logger.error("table={} msg=event_publish_failed", new Object[]{table}, e);
            throw new RuntimeException("msg=event_publish_failed with exception : ", e);
        }
    }

    private boolean isMatching(String str, String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

}
