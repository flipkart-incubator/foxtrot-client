package com.flipkart.foxtrot.client.cluster;

import feign.Headers;
import feign.RequestLine;

public interface FoxtrotClusterHttpClient {

    @RequestLine("GET /foxtrot/v1/cluster/members")
    @Headers("Content-Type: application/json")
    FoxtrotClusterStatus load();

}
