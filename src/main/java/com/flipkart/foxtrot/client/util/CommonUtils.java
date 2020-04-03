package com.flipkart.foxtrot.client.util;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import feign.Client;
import feign.okhttp.OkHttpClient;
import okhttp3.ConnectionPool;
import okhttp3.internal.tls.OkHostnameVerifier;

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
        okhttp3.OkHttpClient client = new okhttp3.OkHttpClient.Builder()
                .retryOnConnectionFailure(true)
                .hostnameVerifier(OkHostnameVerifier.INSTANCE)
                .connectionPool(new ConnectionPool(config.getMaxConnections(), config.getKeepAliveTimeMillis(), TimeUnit.SECONDS))
                .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getOpTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getOpTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();

        return new OkHttpClient(client);
    }
}
