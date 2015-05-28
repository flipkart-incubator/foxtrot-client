package com.flipkart.foxtrot.client.selectors;

import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;

import java.util.List;

public interface MemberSelector {
    public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members);
}
