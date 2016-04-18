package com.flipkart.foxtrot.client.senders;


import feign.Headers;
import feign.Param;
import feign.RequestLine;
import feign.Response;

public interface FoxtrotHttpClient {

    @RequestLine("POST /foxtrot/v1/document/{table}/bulk")
    @Headers({"Content-Type: application/json; charset=utf-8"})
    Response send(@Param("table") String table, byte[] data);
}
