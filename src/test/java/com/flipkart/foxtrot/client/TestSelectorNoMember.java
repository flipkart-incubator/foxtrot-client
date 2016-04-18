/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterStatus;
import com.flipkart.foxtrot.client.selectors.RandomSelector;
import com.flipkart.foxtrot.client.selectors.RoundRobinSelector;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestSelectorNoMember {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8888);

    @Before
    public void setup() throws Exception {
        stubFor(get(urlEqualTo("/foxtrot/v1/cluster/members"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(mapper.writeValueAsBytes(new FoxtrotClusterStatus(
                                Lists.newArrayList(new FoxtrotClusterMember("host1", 18000), new FoxtrotClusterMember("host2", 18000)))))
                        .withHeader("Content-Type", "application/json")));
    }


     @Test
    public void testMemberGet() throws Exception {
        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost("localhost");
        clientConfig.setPort(8888);
        FoxtrotCluster foxtrotCluster = new FoxtrotCluster(clientConfig, new RandomSelector());
        Assert.assertNotNull(foxtrotCluster.member());
    }

    @Test
    public void testMemberGetRR() throws Exception {
        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost("localhost");
        clientConfig.setPort(8888);
        FoxtrotCluster foxtrotCluster = new FoxtrotCluster(clientConfig, new RoundRobinSelector());
        Assert.assertNotNull(foxtrotCluster.member());
    }

}
