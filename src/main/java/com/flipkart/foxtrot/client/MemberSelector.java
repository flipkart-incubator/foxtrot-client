package com.flipkart.foxtrot.client;

import java.util.List;

public interface MemberSelector {
    public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members);
}
