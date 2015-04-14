package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.UUID;

public class FoxtrotClientTest {

    public static void main(String args[]) throws Exception {
        FoxtrotClientConfig config = new FoxtrotClientConfig();
        config.setTable("test");
//        config.setLocalQueuePath("/tmp/foxtrot-messages");
        config.setHost("foxtrot.nm.flipkart.com");
        config.setPort(80);
        FoxtrotClient foxtrotClient = new FoxtrotClient(config);
        JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
        for(int i = 0; i <2000; i++) {
            try {
                foxtrotClient.send(
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
        foxtrotClient.close();
    }
}