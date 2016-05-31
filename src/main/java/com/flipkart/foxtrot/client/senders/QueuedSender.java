package com.flipkart.foxtrot.client.senders;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.EventSender;
import com.flipkart.foxtrot.client.serialization.EventSerializationHandler;
import com.google.common.collect.Lists;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final String path;
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
        this.path = path;
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(path), attr);
        this.messageQueue = new BigQueueImpl(path, "foxtrot-messages");
        this.messageSenderThread = new MessageSenderThread(this, eventSender, messageQueue, path, getSerializationHandler(), batchSize);
        this.scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleWithFixedDelay(messageSenderThread, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(new QueueCleaner(messageQueue, path), 0, 15, TimeUnit.SECONDS);
    }

    @Override
    public void send(Document document) throws Exception {
        this.messageQueue.enqueue(getSerializationHandler().serialize(document));
    }

    @Override
    public void send(String table, Document document) throws Exception {
        throw new IllegalAccessException("Send to table is not implemented for queued sender");
    }

    @Override
    public void send(List<Document> documents) throws Exception {
        for (Document document : documents) {
            this.messageQueue.enqueue(getSerializationHandler().serialize(document));
        }
    }

    @Override
    public void send(String table, List<Document> documents) throws Exception {
        throw new IllegalAccessException("Send to table is not implemented for queued sender");
    }

    @Override
    public void close() throws Exception {
        logger.info("queue={} closing_queued_sender", new Object[]{path});
        while (!messageQueue.isEmpty()) {
            Thread.sleep(500);
            logger.info("queue={} message_queue_not_empty waiting_for_queue_to_get_empty", new Object[]{path});
        }

        while (messageSenderThread.isRunning()) {
            Thread.sleep(500);
            logger.info("queue={} message_sender_thread_still_running waiting_for_completion", new Object[]{path});
        }
        this.scheduler.shutdownNow();
        logger.info("queue={} shutting_down_message_sender_thread", new Object[]{path});
        this.eventSender.close();
        logger.info("queue={} shutdown_completed_for_message_sender_thread", new Object[]{path});
    }

    private static final class MessageSenderThread implements Runnable {
        private final QueuedSender sender;
        private final EventSerializationHandler serializationHandler;
        private EventSender eventSender;
        private IBigQueue messageQueue;
        private int batchSize;
        private String path;
        private AtomicBoolean running = new AtomicBoolean(false);

        public MessageSenderThread(QueuedSender queuedSender,
                                   EventSender eventSender,
                                   IBigQueue messageQueue,
                                   String path,
                                   EventSerializationHandler serializationHandler,
                                   int batchSize) throws Exception {
            this.sender = queuedSender;
            this.eventSender = eventSender;
            this.messageQueue = messageQueue;
            this.path = path;
            this.serializationHandler = serializationHandler;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            running.set(true);
            try {
                while (!messageQueue.isEmpty()) {
                    logger.info("queue={} messages_found_in_message_queue sender_invoked", new Object[]{path});
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
                                logger.error("queue={} message_size_limit_exceeded(2MB) message={}", new Object[]{path, new String(data)});
                                continue; //Move to next message
                            } else {
                                logger.info("queue={} batch_data_size_exceeds_threshold(2MB) size={} truncating_batch_size enqueuing_last_message_for_next_batch",
                                        new Object[]{path, sizeOfPayload});
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
                                logger.info("queue={} foxtrot_messages_sent count={}", new Object[]{path, entries.size()});
                                break;
                            } catch (Throwable t) {
                                logger.error("queue={} message_send_failed count={}", new Object[]{path, entries.size()}, t);
                            }
                        } while (retryCount <= RETRIES);
                        if (retryCount > RETRIES) {
                            logger.error("queue={} message_send_failed probably_api_down  re-queuing_messages", new Object[]{path});
                            sender.send(entries);
                            break;
                        }
                    } else {
                        logger.info("queue={} nothing_to_send_to_foxtrot", new Object[]{path});
                    }
                }
            } catch (Exception e) {
                logger.error("queue={} message_send_failed", new Object[]{path}, e);
            }
            running.set(false);
        }

        private boolean isRunning() {
            return running.get();
        }
    }

    private static final class QueueCleaner implements Runnable {
        private IBigQueue messageQueue;
        private String path;

        private QueueCleaner(IBigQueue messageQueue, String path) {
            this.messageQueue = messageQueue;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                long startTime = System.currentTimeMillis();
                this.messageQueue.gc();
                logger.info("queue={} ran_gc_on_foxtrot_message_queue took={}", new Object[]{path, System.currentTimeMillis() - startTime});
            } catch (Exception e) {
                logger.error("queue={} gc_failed_on_foxtrot_message_queue", new Object[]{path}, e);
            }
        }
    }

}
