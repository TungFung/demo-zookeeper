package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CreateSessionSample {

    public static void main(String[] args) throws Exception {
        //zk服务器列表
        String connectString = "master:2181,slave1:2181,slave2:2181";

        //baseSleepTimeMs:初始sleep时间，maxRetries:最大重试次数，maxSleepMs:最大sleep时间
        //随着重试次数的增加，计算出的sleep时间会越来越大。如果该sleep时间在maxSleepMs的范围内，那么使用该sleep时间，否则使用maxSleepMs,另外限制了最大重试次数防止无限重试
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        //sessionTimeoutMs:会话超时时间，默认60 000ms,connectionTimeoutMs:连接创建超时时间，默认15 000ms
        //retryPolicy:重试策略，ExponentialBackoffRetry、RetryNTimes、RetryOneTime、RetryUntilElapsed
        //CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, 5000, 3000, retryPolicy);

        //Fluent风格创建会话
        //namespace:为了实现业务间的隔离，往往会给每个业务分配一个独立的命名空间，即一个zk根路径,在第一次创建节点的时候才会创建这个根节点
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(retryPolicy)
                .namespace("businessBase")
                .build();

        client.start();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
