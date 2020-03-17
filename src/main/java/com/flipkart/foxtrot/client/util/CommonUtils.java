package com.flipkart.foxtrot.client.util;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.squareup.okhttp.internal.tls.OkHostnameVerifier;
import feign.Client;
import feign.okhttp.OkHttpClient;
import okhttp3.ConnectionPool;

import java.util.concurrent.TimeUnit;

public class CommonUtils {

    private CommonUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Creates OkHttpClient for Feign
     *
     * @param config {@link FoxtrotClientConfig}
     * @return {@link Client}
     */
    public static Client createOkHttpClient(FoxtrotClientConfig config) {
        okhttp3.OkHttpClient.Builder clientBuilder = new okhttp3.OkHttpClient.Builder();
        clientBuilder.retryOnConnectionFailure(true);
        int connections = config.getMaxConnections();
        int callTimeOutMs = config.getCallTimeOutMs();
        int connTimeout = config.getConnectTimeoutMs();
        int opTimeout = config.getOpTimeoutMs();
        okhttp3.OkHttpClient client = clientBuilder.hostnameVerifier(OkHostnameVerifier.INSTANCE)
                .connectionPool(new ConnectionPool(connections, config.getKeepAliveTimeMillis(), TimeUnit.SECONDS))
                .connectTimeout(connTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(opTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(opTimeout, TimeUnit.MILLISECONDS)
                .callTimeout(callTimeOutMs, TimeUnit.MILLISECONDS)
                .build();
        return new OkHttpClient(client);
    }
}
