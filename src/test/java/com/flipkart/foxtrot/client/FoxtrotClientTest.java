package com.flipkart.foxtrot.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.selectors.RoundRobinSelector;

public class FoxtrotClientTest {

    /*public static void main(String args[]) throws Exception {
        FoxtrotClientConfig config = new FoxtrotClientConfig();
        config.setHost("foxtrot.nm.flipkart.com");
        config.setPort(80);
        FoxtrotClient client = new FoxtrotClient(config, new RoundRobinSelector());
        Thread.sleep(10000);
        for(int i = 0; i <20; i++) {
            System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(client.clusterMember()));
        }
    }*/
}