package com.flipkart.foxtrot.client.selectors;

import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSelector implements MemberSelector {

    @Override
    public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members) {
        return members.get(ThreadLocalRandom.current().nextInt(members.size()));
    }
}
