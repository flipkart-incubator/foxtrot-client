package com.flipkart.foxtrot.client.cluster;

import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.flipkart.foxtrot.client.selectors.MemberSelector;
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
    private FoxtrotClientConfig clientConfig;
    private final ScheduledFuture<?> future;
    private ScheduledExecutorService executorService;
    private AtomicReference<FoxtrotClusterStatus> status = new AtomicReference<>();

    public FoxtrotCluster(FoxtrotClientConfig config,
                          MemberSelector selector) throws Exception {
        this.selector = selector;
        this.clientConfig = config;
        executorService = Executors.newScheduledThreadPool(1);
        ClusterStatusUpdater updater = ClusterStatusUpdater.create(config, status);
        updater.loadClusterData();
        future = executorService.scheduleWithFixedDelay(updater, config.getRefreshIntervalSecs(),
                config.getRefreshIntervalSecs(), TimeUnit.SECONDS);
    }

    public FoxtrotClusterMember member() {
        if (null == status || status.get() == null || status.get().getMembers().isEmpty()) {
            return null;
        }
        return selector.selectMember(status.get().getMembers());
    }

    public void stop() {
        logger.info("table={} shutting_down_cluster_status_checker", new Object[]{clientConfig.getTable()});
        future.cancel(true);
        while (!future.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error("table={} interrupted", new Object[]{clientConfig.getTable()}, e);
            }
            logger.info("table={} waiting_for_checker_to_stop", new Object[]{clientConfig.getTable()});
        }
        executorService.shutdown();
        //Wait for the running tasks
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("table={} executor_service_termination_exception", new Object[]{clientConfig.getTable()}, e);
        }
        executorService.shutdownNow();
        logger.info("table={} cluster_status_checker_shutdown_complete", new Object[]{clientConfig.getTable()});
    }
}
