package com.flipkart.foxtrot.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FoxtrotClient {
    private final MemberSelector selector;
    private ScheduledExecutorService executorService;
    private AtomicReference<FoxtrotClusterStatus> status = new AtomicReference<>();

    public FoxtrotClient(FoxtrotClientConfig config, MemberSelector selector) throws Exception {
        this.selector = selector;
        executorService = Executors.newScheduledThreadPool(1);
        ClusterStatusUpdater updater = ClusterStatusUpdater.create(config, status);
        executorService.scheduleWithFixedDelay(updater, 0, config.getRefreshInterval(), TimeUnit.SECONDS);
    }

    public FoxtrotClusterMember clusterMember() {
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
        executorService.shutdown();
    }
}
