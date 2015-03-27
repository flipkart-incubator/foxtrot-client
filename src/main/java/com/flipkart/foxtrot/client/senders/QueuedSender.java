package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.EventSerializationHandler;
import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.serialization.JacksonJsonSerializationHandler;
import com.google.common.collect.Lists;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An {@link com.flipkart.foxtrot.client.EventSender} that uses a persistent queue to save and forward messages.
 * During send, a push to the queue is done. A separate thread batches and forwards the messages to hyperion.
 * If push fails, it will keep on retrying. check the constructors for different options. This sender allows for setting a
 * lower-level sender like {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender} to send the events to the API.
 * If no sender is set, it creates an {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender} to send messages.
 */
public class QueuedSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(QueuedSender.class.getSimpleName());
    private static final int RETRIES = 5;
    private static final int DEFAULT_BATCH_SIZE = 200;
    private static final int MAX_PAYLOAD_SIZE = 2000000; //2MB
    private static final String DEFAULT_PATH = "/tmp/foxtrot-messages";
    private final EventSender eventSender;

    private IBigQueue messageQueue;
    private final ScheduledExecutorService scheduler;


    public QueuedSender(final String appName, FoxtrotCluster cluster) throws Exception {
        this(appName, cluster, DEFAULT_PATH);
    }

    public QueuedSender(final String appName, FoxtrotCluster cluster, final String path) throws Exception {
        this(appName, cluster, path, DEFAULT_BATCH_SIZE);
    }

    public QueuedSender(final String appName, FoxtrotCluster cluster, final String path, int batchSize) throws Exception {
        this(new HttpSyncEventSender(appName, cluster), path, batchSize);
    }

    public QueuedSender(EventSender eventSender, final String path) throws Exception {
        this(eventSender, path, DEFAULT_BATCH_SIZE);
    }

    public QueuedSender(EventSender eventSender, final String path, int batchSize) throws Exception {
        this(eventSender, JacksonJsonSerializationHandler.INSTANCE, path, batchSize, 1);
    }

    /**
     * Instantiates a new Queued sender.
     *
     * @param eventSender Set a sender like {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender}.
     * @param path The path to the queue file.
     * @param batchSize The size of the batch to be sent per API call.
     * @param numSecondsBetweenRefresh the num seconds between refresh
     * @throws Exception the exception
     */
    public QueuedSender(EventSender eventSender,
                        EventSerializationHandler serializationHandler,
                        final String path,
                        int batchSize,
                        int numSecondsBetweenRefresh) throws Exception {
        super(serializationHandler);
        this.eventSender = eventSender;
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(path), attr);
        this.messageQueue = new BigQueueImpl(path, "foxtrot-messages");
        MessageSenderThread messageSenderThread = new MessageSenderThread(this, eventSender, messageQueue, getSerializationHandler(), batchSize);
        this.scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleWithFixedDelay(messageSenderThread, 0,
                numSecondsBetweenRefresh, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new QueueCleaner(messageQueue), 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void send(Document event) throws Exception {
        this.messageQueue.enqueue(this.getSerializationHandler().serialize(event));
    }

    @Override
    public void send(List<Document> events) throws Exception {
        this.messageQueue.enqueue(this.getSerializationHandler().serialize(events));
    }

    @Override
    public void close() throws Exception {
        while (!messageQueue.isEmpty()) {
            Thread.sleep(1000);
            logger.debug("Message queue is not empty .. waiting");
        }
        this.scheduler.shutdownNow();
        logger.debug("Shut down sender thread");
        this.eventSender.close();
        logger.debug("Shut down scheduled sender");
    }

    private static final class MessageSenderThread implements Runnable {
        private final QueuedSender sender;
        private EventSender eventSender;
        private IBigQueue messageQueue;
        private final EventSerializationHandler serializationHandler;
        private int batchSize;
        private int sizeOfPayload;

        public MessageSenderThread(QueuedSender queuedSender, EventSender eventSender, IBigQueue messageQueue,
                                   EventSerializationHandler serializationHandler, int batchSize) throws Exception {
            this.sender = queuedSender;
            this.eventSender = eventSender;
            this.messageQueue = messageQueue;
            this.serializationHandler = serializationHandler;
            this.batchSize = batchSize;
            this.sizeOfPayload=0;
        }

        @Override
        public void run() {
            try {
                while(!messageQueue.isEmpty()) {
                    logger.info("There are messages in the hyperion message queue. Sender invoked.");
                    List<Document> entries = Lists.newArrayListWithExpectedSize(batchSize);
                    for(int i = 0; i < batchSize; i++) {
                        byte data[] = messageQueue.dequeue();
                        if(null == data) {
                            break;
                        }
                        //check added to keep avoid payload size greater than 2MB from being pushed in one batch calls
                        this.sizeOfPayload += data.length+24+8;
                        if(sizeOfPayload > MAX_PAYLOAD_SIZE){
                            if(data.length +24+8 > MAX_PAYLOAD_SIZE){
                                logger.error(String.format("Dropping packet as size > 2MB  packet : %s",new String(data)));
                                break;
                            }else{
                                logger.info(String.format("data size %d > 2MB threshold, hence truncating batch size for this and enqueing the last overriding data to pass on in next batch.",sizeOfPayload));
                                messageQueue.enqueue(data);
                                break;
                            }
                        }
                        entries.add(serializationHandler.deserialize(data));
                    }
                    if (!entries.isEmpty()) {
                        //System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(entries));
                        int retryCount = 0;
                        do {
                            retryCount++;
                            try {
                                eventSender.send(entries);
                                logger.info(String.format("Sent %d events to hyperion.", entries.size()));
                                break;
                            } catch (Throwable t) {
                                logger.error("Could not send events: ", t);
                            }
                        } while (retryCount <= RETRIES);
                        if(retryCount > RETRIES) {
                            logger.error("Could not send event. Probably hyperion api is down. Re-queuing the messages." +
                                    " Order will be screwed up. But will appear proper on graph once ingested.");
                            sender.send(entries);
                            break;
                        }
                    }
                    else {
                        logger.info("Nothing to send to hyperion");
                    }
                }
            } catch (Exception e) {
                logger.error("Could not send message: ", e);
            }
        }
    }

    private static final class QueueCleaner implements Runnable {
        private IBigQueue messageQueue;

        private QueueCleaner(IBigQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            try {
                long startTime = System.currentTimeMillis();
                this.messageQueue.gc();
                logger.info(String.format("Ran GC on queue. Took: %d milliseconds",
                        (System.currentTimeMillis() - startTime)));
            } catch (IOException e) {
                logger.error("Could not perform GC on hyperion message queue: ", e);
            }
        }
    }

}
