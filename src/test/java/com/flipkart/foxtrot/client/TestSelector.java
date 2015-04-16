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
import com.flipkart.foxtrot.client.serialization.JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl;
import com.google.common.collect.Lists;
import org.apache.http.HttpException;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestSelector {
    private static final ObjectMapper mapper = new ObjectMapper();
    private LocalTestServer localTestServer = new LocalTestServer(null, null);

    @Before
    public void setup() throws Exception {
        localTestServer.register("/foxtrot/v1/cluster/members", new HttpRequestHandler() {
            @Override
            public void handle(org.apache.http.HttpRequest request, org.apache.http.HttpResponse response, HttpContext context) throws HttpException, IOException {
                response.setEntity(new StringEntity(mapper.writeValueAsString(new FoxtrotClusterStatus(
                        Lists.newArrayList(new FoxtrotClusterMember("host1", 18000), new FoxtrotClusterMember("host2", 18000))
                ))));
                response.setStatusCode(200);
            }
        });
        localTestServer.start();
    }

    @After
    public void tearDown() throws Exception {
        localTestServer.stop();
    }

    @Test
    public void testMemberGet() throws Exception {
        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost(localTestServer.getServiceAddress().getHostName());
        clientConfig.setPort(localTestServer.getServiceAddress().getPort());
        FoxtrotCluster foxtrotCluster = new FoxtrotCluster(clientConfig, new RandomSelector(), JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl.INSTANCE);
        Assert.assertNotNull(foxtrotCluster.member());
    }

    @Test
    public void testMemberGetRR() throws Exception {
        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost(localTestServer.getServiceAddress().getHostName());
        clientConfig.setPort(localTestServer.getServiceAddress().getPort());
        FoxtrotCluster foxtrotCluster = new FoxtrotCluster(clientConfig, new RoundRobinSelector(), JacksonJsonFoxtrotClusterResponseSerializationHandlerImpl.INSTANCE);
        final String host1 = foxtrotCluster.member().getHost();
        final String host2 = foxtrotCluster.member().getHost();
        Assert.assertNotNull(host1);
        Assert.assertNotNull(host2);
        Assert.assertNotEquals(host1, host2);
    }

}
