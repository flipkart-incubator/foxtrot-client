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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.selectors.MemberSelector;
import com.flipkart.foxtrot.client.serialization.JacksonJsonSerializationHandler;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class EventQueuedSendTest extends BaseTest {

    private TestHostPort testHostPort = new TestHostPort("localhost", 17000);

    @Test
    public void testQueuedSend() throws Exception {
        Path tmpPath = Files.createTempDirectory("tmp");
        tmpPath.toFile().deleteOnExit();
        final String path = tmpPath.toString();


        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost(testHostPort.getHostName());
        clientConfig.setPort(testHostPort.getPort());
        clientConfig.setTable("test");
        clientConfig.setQueuePath(path);
        clientConfig.setBatchSize(50);

        FoxtrotClient client = new FoxtrotClient(clientConfig, new MemberSelector() {
            @Override
            public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members) {
                return new FoxtrotClusterMember(testHostPort.getHostName(), testHostPort.getPort());
            }
        }, JacksonJsonSerializationHandler.INSTANCE);
        JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
        for (int i = 0; i < 200; i++) {
            try {
                client.send(
                        new Document(
                                UUID.randomUUID().toString(),
                                System.currentTimeMillis(),
                                new ObjectNode(nodeFactory)
                                        .put("testField", "Santanu Sinha")
                        )
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Thread.sleep(10000);
        verify(200, postRequestedFor(urlEqualTo("/foxtrot/v1/document/test/bulk")));
        client.close();
    }

    @Test
    public void testQueuedSendBulk() throws Exception {
        Path tmpPath = Files.createTempDirectory("tmp");
        tmpPath.toFile().deleteOnExit();
        final String path = tmpPath.toString();


        FoxtrotClientConfig clientConfig = new FoxtrotClientConfig();
        clientConfig.setHost(testHostPort.getHostName());
        clientConfig.setPort(testHostPort.getPort());
        clientConfig.setTable("test");
        clientConfig.setQueuePath(path);
        clientConfig.setBatchSize(50);

        FoxtrotClient client = new FoxtrotClient(clientConfig, new MemberSelector() {
            @Override
            public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members) {
                return new FoxtrotClusterMember(testHostPort.getHostName(), testHostPort.getPort());
            }
        }, JacksonJsonSerializationHandler.INSTANCE);
        JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                            System.currentTimeMillis(),
                            new ObjectNode(nodeFactory).put("testField", "Rishabh Goyal")
                    )
            );
        }
        try {
            client.send(documents);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(10000);
        verify(1, postRequestedFor(urlEqualTo("/foxtrot/v1/document/test/bulk")));
        client.close();
    }
}
