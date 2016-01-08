package com.flipkart.foxtrot.client.senders.impl;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by santanu.s on 06/01/16.
 */
public class CustomKeepAliveStrategy implements ConnectionKeepAliveStrategy {
    private static final Logger logger = LoggerFactory.getLogger(CustomKeepAliveStrategy.class);

    private final FoxtrotClientConfig config;

    public CustomKeepAliveStrategy(FoxtrotClientConfig config) {
        this.config = config;
    }

    @Override
    public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
        final HeaderElementIterator it = new BasicHeaderElementIterator(
                httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE));
        while (it.hasNext()) {
            final HeaderElement he = it.nextElement();
            final String param = he.getName();
            final String value = he.getValue();
            if (value != null && param.equalsIgnoreCase("timeout")) {
                try {
                    final long timeoutSeconds = Long.parseLong(value);
                    logger.info("Setting keep alive to: {} seconds", timeoutSeconds);
                    return timeoutSeconds * 1000;
                } catch(final NumberFormatException ignore) {
                }
            }
        }
        logger.debug("Did not get a keep alive from server");
        logger.debug("Setting keep alive to: {} seconds", config.getKeepAliveTimeMillis() / 1000);
        return config.getKeepAliveTimeMillis();
    }
}
