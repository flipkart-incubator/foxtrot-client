package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterStatus;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;

import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class BaseTest {

    final ObjectMapper objectMapper = new ObjectMapper();

    final FoxtrotClusterStatus clusterStatus = new FoxtrotClusterStatus(Collections.singletonList(new FoxtrotClusterMember("localhost", 8080)));

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8888);

    @Before
    public void setup() throws JsonProcessingException {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        stubFor(get(urlEqualTo("/foxtrot/v1/cluster/members"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(objectMapper.writeValueAsBytes(clusterStatus))
                        .withHeader("Content-Type", "application/json")));
        stubFor(post(urlEqualTo("/foxtrot/v1/document/test/bulk"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withBody(objectMapper.writeValueAsBytes(clusterStatus))
                        .withHeader("Content-Type", "application/json")));


    }
}
