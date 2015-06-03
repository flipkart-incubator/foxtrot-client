package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link com.flipkart.foxtrot.client.EventSender} that uses a persistent queue to save and forward messages.
 * During send, a push to the queue is done. A separate thread batches and forwards the messages to foxtrot.
 * If push fails, it will keep on retrying. check the constructors for different options. This sender allows for setting a
 * lower-level sender like {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender} to send the events to the API.
 * If no sender is set, it creates an {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender} to send messages.
 */
public class QueuedSender extends EventSender {
    private static final Logger logger = LoggerFactory.getLogger(QueuedSender.class.getSimpleName());
    private static final int RETRIES = 5;
    private static final int MAX_PAYLOAD_SIZE = 2000000; //2MB
    private final EventSender eventSender;
    private final MessageSenderThread messageSenderThread;
    private final ScheduledExecutorService scheduler;
    private IBigQueue messageQueue;


    /**
     * Instantiates a new Queued sender.
     *
     * @param eventSender Set a sender like {@link com.flipkart.foxtrot.client.senders.HttpSyncEventSender}.
     * @param path        The path to the queue file.
     * @param batchSize   The size of the batch to be sent per API call.
     * @throws Exception the exception
     */
    public QueuedSender(EventSender eventSender,
                        EventSerializationHandler serializationHandler,
                        final String path,
                        int batchSize) throws Exception {
        super(serializationHandler);
        this.eventSender = eventSender;
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(path), attr);
        this.messageQueue = new BigQueueImpl(path, "foxtrot-messages");
        this.messageSenderThread = new MessageSenderThread(this, eventSender, messageQueue, getSerializationHandler(), batchSize);
        this.scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleWithFixedDelay(messageSenderThread, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(new QueueCleaner(messageQueue), 0, 15, TimeUnit.SECONDS);
    }

    @Override
    public void send(Document document) throws Exception {
        this.messageQueue.enqueue(getSerializationHandler().serialize(document));
    }

    @Override
    public void send(List<Document> documents) throws Exception {
        for (Document document : documents) {
            this.messageQueue.enqueue(getSerializationHandler().serialize(document));
        }
    }

    @Override
    public void close() throws Exception {
        while (!messageQueue.isEmpty()) {
            Thread.sleep(1000);
            logger.debug("Message queue is not empty .. waiting");
        }

        while (messageSenderThread.isRunning()) {
            Thread.sleep(500);
            logger.debug("Message sender thread is still running.. waiting");
        }
        this.scheduler.shutdownNow();
        logger.debug("Shut down sender thread");
        this.eventSender.close();
        logger.debug("Shut down scheduled sender");
    }

    private static final class MessageSenderThread implements Runnable {
        private final QueuedSender sender;
        private final EventSerializationHandler serializationHandler;
        private EventSender eventSender;
        private IBigQueue messageQueue;
        private int batchSize;
        private AtomicBoolean running = new AtomicBoolean(false);

        public MessageSenderThread(QueuedSender queuedSender, EventSender eventSender, IBigQueue messageQueue,
                                   EventSerializationHandler serializationHandler, int batchSize) throws Exception {
            this.sender = queuedSender;
            this.eventSender = eventSender;
            this.messageQueue = messageQueue;
            this.serializationHandler = serializationHandler;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            running.set(true);
            try {
                while (!messageQueue.isEmpty()) {
                    logger.info("There are messages in the foxtrot message queue. Sender invoked.");
                    List<Document> entries = Lists.newArrayListWithExpectedSize(batchSize);
                    int sizeOfPayload = 0;
                    for (int i = 0; i < batchSize; i++) {
                        byte data[] = messageQueue.dequeue();
                        if (null == data) {
                            break;
                        }
                        // Check added to keep avoid payload size greater than 2MB from being pushed in one batch calls
                        sizeOfPayload += data.length + 24 + 8;
                        if (sizeOfPayload > MAX_PAYLOAD_SIZE) {
                            if (data.length + 24 + 8 > MAX_PAYLOAD_SIZE) { //A single message > 2MB..
                                logger.error(String.format("Dropping message as size > 2MB  packet : %s", new String(data)));
                                continue; //Move to next message
                            } else {
                                logger.info(String.format("data size %d > 2MB threshold, hence truncating batch size for this and enqueing the last overriding data to pass on in next batch.", sizeOfPayload));
                                messageQueue.enqueue(data);
                                break;
                            }
                        }
                        entries.add(serializationHandler.deserialize(data));
                    }
                    if (!entries.isEmpty()) {
                        int retryCount = 0;
                        do {
                            retryCount++;
                            try {
                                eventSender.send(entries);
                                logger.info(String.format("Sent %d events to foxtrot.", entries.size()));
                                break;
                            } catch (Throwable t) {
                                logger.error("Could not send events: ", t);
                            }
                        } while (retryCount <= RETRIES);
                        if (retryCount > RETRIES) {
                            logger.error("Could not send event. Probably foxtrot api is down. Re-queuing the messages." +
                                    " Order will be screwed up. But will appear proper on graph once ingested.");
                            sender.send(entries);
                            break;
                        }
                    } else {
                        logger.info("Nothing to send to foxtrot");
                    }
                }
            } catch (Exception e) {
                logger.error("Could not send message: ", e);
            }
            running.set(false);
        }

        private boolean isRunning() {
            return running.get();
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
                logger.error("Could not perform GC on foxtrot message queue: ", e);
            }
        }
    }

}
