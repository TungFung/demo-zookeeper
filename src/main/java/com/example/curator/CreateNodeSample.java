package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class CreateNodeSample {

    public static void main(String[] args) throws Exception {
        //zk服务器列表
        String connectString = "master:2181,slave1:2181,slave2:2181";

        //baseSleepTimeMs:初始sleep时间，maxRetries:最大重试次数，maxSleepMs:最大sleep时间
        //随着重试次数的增加，计算出的sleep时间会越来越大。如果该sleep时间在maxSleepMs的范围内，那么使用该sleep时间，否则使用maxSleepMs,另外限制了最大重试次数防止无限重试
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(retryPolicy)
                .namespace("businessBase")
                .build();

        client.start();

        //创建了/businessBase/my_test_node节点
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/my_test_node/sec_node", "this is an example".getBytes());
    }
}
