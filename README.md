# Foxtrot Client [![Travis build status](https://travis-ci.org/flipkart-incubator/foxtrot-client.svg?branch=master)](https://travis-ci.org/flipkart-incubator/foxtrot-client)

This libary is a smart client to the [Foxtrot](https://travis-ci.org/flipkart-incubator/foxtrot) event storage and analytics framework.
- Uses service discovery to find foxtrot nodes
- Maintains connection pool to individual nodes
- Maintains local cache of nodes
- Provides configurable node selectors to send a message
- Provides multiple types of event senders:
    - Synchronous event sender. It sends one or more events directly to Foxtrot.
    - Queued event sender.
        - Uses a persistent disk based queue.
        - Uses a syncronous sender on a separate thread to batch and send events to Foxtrot.
     
     
## Usage

Use the following repository in your pom.xml:

    <repository>
        <id>clojars</id>
        <name>Clojars repository</name>
        <url>https://clojars.org/repo</url>
    </repository>

Use the following maven dependency:

    <dependency>
      <groupId>com.flipkart.foxtrot</groupId>
      <artifactId>foxtrot-client</artifactId>
      <version>0.1-SNAPSHOT</version>
    </dependency>
    
## Show me the code

Initialize the Client like this:

    FoxtrotClientConfig config = new FoxtrotClientConfig();
    config.setTable("test");                           //Your foxtrot table name
    config.setLocalQueuePath("/tmp/foxtrot-messages"); //Giving this path means it will use the queued sender
    config.setHost("foxtrot.yourdomain.com");          //Load balancer hostname/ip
    config.setPort(80);                                //Load balancer port
    
    FoxtrotClient foxtrotClient = new FoxtrotClient(config);
    
Send events:

    foxtrotClient.send(
                        new Document(
                                    UUID.randomUUID().toString(),             //ID
                                    System.currentTimeMillis(),               //Timestamp
                                    new ObjectNode(nodeFactory)               //Data
                                            .put("testField", "Santanu Sinha") 
                        )
    );

Close when done (program stop):

    foxtrotClient.close();
