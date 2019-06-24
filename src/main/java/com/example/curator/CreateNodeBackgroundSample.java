package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Curator提供了异步接口操作,通过BackgroundCallback接收异步回调结果
 * BackgroundCallback接收CuratorFramework client,CuratorEvent event两个参数
 * 重点在于CuratorEvent中的事件类型和响应码
 * 事件类型：getType()
 *  CREATE、DELETE、EXISTS、GET_DATA、SET_DATA、CHILDREN、SYNC、GET_ACL、WATCHED、CLOSING
 *
 * 响应码: getResultCode()
 *  0 -- OK
 *  -1 -- ConnectionLoss
 *  -110 -- NodeExists
 *  -112 -- SessionExpired
 */
public class CreateNodeBackgroundSample {

    private static Logger log = LoggerFactory.getLogger(CreateNodeBackgroundSample.class);

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

        CountDownLatch latch = new CountDownLatch(2);
        ExecutorService threadPool = Executors.newFixedThreadPool(2);

        client.start();

        String nodePath = "/my_test_node";

        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                log.info("event[code:{},type:{}]", curatorEvent.getResultCode(), curatorEvent.getType());//event[code:0,type:CREATE]
                latch.countDown();
            }
        }, threadPool).forPath(nodePath, "this is an example".getBytes());//这个创建成功，而且是采用线程池异步处理创建结果的

        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                log.info("event[code:{},type:{}]", curatorEvent.getResultCode(), curatorEvent.getType());//event[code:-110,type:CREATE]
                latch.countDown();
            }
        }).forPath(nodePath, "this is an example".getBytes());//这个会创建失败,采用的是EventThread来处理创建结果

        latch.await();
        threadPool.shutdown();
    }
}
