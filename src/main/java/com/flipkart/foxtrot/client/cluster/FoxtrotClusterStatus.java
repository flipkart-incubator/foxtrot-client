package com.flipkart.foxtrot.client.cluster;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class FoxtrotClusterStatus {
    private List<FoxtrotClusterMember> members;

    public FoxtrotClusterStatus(final FoxtrotClusterStatus rhs) {
        members = ImmutableList.copyOf(rhs.getMembers());
    }

    public FoxtrotClusterStatus() {
    }

    public List<FoxtrotClusterMember> getMembers() {
        return members;
    }

    public void setMembers(List<FoxtrotClusterMember> members) {
        this.members = members;
    }
}
