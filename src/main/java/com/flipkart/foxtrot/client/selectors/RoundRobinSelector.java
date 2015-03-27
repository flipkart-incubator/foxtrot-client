package com.flipkart.foxtrot.client.selectors;

import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.MemberSelector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinSelector implements MemberSelector {
    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members) {
        return members.get(counter.getAndSet((counter.get() + 1 ) % members.size()));
    }
}
