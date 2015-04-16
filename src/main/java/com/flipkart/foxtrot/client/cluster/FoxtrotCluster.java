package com.flipkart.foxtrot.client.cluster;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.MemberSelector;
import com.flipkart.foxtrot.client.serialization.FoxtrotClusterResponseSerializationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FoxtrotCluster {
    private static final Logger logger = LoggerFactory.getLogger(FoxtrotCluster.class.getSimpleName());

    private final MemberSelector selector;
    private final ScheduledFuture<?> future;
    private ScheduledExecutorService executorService;
    private AtomicReference<FoxtrotClusterStatus> status = new AtomicReference<>();

    public FoxtrotCluster(FoxtrotClientConfig config,
                   MemberSelector selector,
                   FoxtrotClusterResponseSerializationHandler serializationHandler) throws Exception {
        this.selector = selector;
        executorService = Executors.newScheduledThreadPool(1);
        ClusterStatusUpdater updater = ClusterStatusUpdater.create(config, status, serializationHandler);
        updater.loadClusterData();
        future = executorService.scheduleWithFixedDelay(updater, config.getRefreshIntervalSecs(),
                                                                config.getRefreshIntervalSecs(), TimeUnit.SECONDS);
    }

    public FoxtrotClusterMember member() {
        if(null == status) {
            return null;
        }
        FoxtrotClusterStatus foxtrotClusterStatus = status.get();
        if(null == foxtrotClusterStatus || foxtrotClusterStatus.getMembers().isEmpty()) {
            return null;
        }
        return selector.selectMember(foxtrotClusterStatus.getMembers());
    }

    public void stop() {
        logger.debug("Shutting down cluster status checker");
        future.cancel(true);
        while (!future.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            }
            logger.debug("Waiting for checker to stop");
        }
        executorService.shutdown();
        //Wait for the running tasks
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("executor_service_termination_exception", e);
        }

        executorService.shutdownNow();
        logger.debug("Shut down cluster status checker");
    }
}
