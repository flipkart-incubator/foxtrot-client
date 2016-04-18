package com.flipkart.foxtrot.client.selectors;

import com.flipkart.foxtrot.client.cluster.FoxtrotCluster;
import com.flipkart.foxtrot.client.cluster.FoxtrotClusterMember;
import feign.Request;
import feign.RequestTemplate;
import feign.Target;

public class FoxtrotTarget<T> implements Target<T> {

    private final Class<T> type;
    private final String name;
    private final FoxtrotCluster foxtrotCluster;

    public FoxtrotTarget(Class<T> type, final String name, FoxtrotCluster foxtrotCluster) {
        this.type = type;
        this.name = name;
        this.foxtrotCluster = foxtrotCluster;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String url() {
        FoxtrotClusterMember member = foxtrotCluster.member();
        return String.format("http://%s:%d", member.getHost(), member.getPort());
    }

    @Override
    public Request apply(RequestTemplate input) {
        FoxtrotClusterMember member = foxtrotCluster.member();
        String url = String.format("http://%s:%d", member.getHost(), member.getPort());
        input.insert(0, url);
        return input.request();
    }
}
