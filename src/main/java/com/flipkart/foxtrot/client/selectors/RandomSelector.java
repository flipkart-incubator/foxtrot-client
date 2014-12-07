package com.flipkart.foxtrot.client.selectors;

import com.flipkart.foxtrot.client.FoxtrotClusterMember;
import com.flipkart.foxtrot.client.MemberSelector;

import java.util.List;
import java.util.Random;

public class RandomSelector implements MemberSelector{
    private Random random = new Random(System.currentTimeMillis());

    @Override
    public FoxtrotClusterMember selectMember(List<FoxtrotClusterMember> members) {
        return members.get(Math.abs(random.nextInt()) % members.size());
    }
}
