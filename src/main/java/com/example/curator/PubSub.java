package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 发布订阅
 * 利用zk节点变化watcher通知的特性实现发布订阅
 */
public class PubSub {

    private static Logger log = LoggerFactory.getLogger(PubSub.class);

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

        //NodeCache监听
        String nodePath = "/pub_node";
        NodeCache nodeCache = new NodeCache(client, nodePath, false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if(nodeCache.getCurrentData() == null){
                    log.info("节点被删除了");
                    return;
                }

                log.info("节点数据改变，新数据内容:{}", new String(nodeCache.getCurrentData().getData()));
            }
        });

        //创建节点，不会触发回调
        client.create().creatingParentsIfNeeded().forPath(nodePath, "some infos".getBytes());

        //修改节点，NodeCacheListener会回调
        client.setData().forPath(nodePath, "has been updated".getBytes());

        //删除节点,不会触发回调
        client.delete().deletingChildrenIfNeeded().forPath(nodePath);

        //PathChildrenCache监听
        String nodePath2 = "/pub_node2";
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, nodePath2, true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()){
                    case INITIALIZED:
                        log.info("INITIALIZED:{}", event.getData());
                        break;
                    case CHILD_ADDED:
                        log.info("CHILD_ADDED:{}", event.getData());
                        break;
                    case CHILD_UPDATED:
                        log.info("CHILD_UPDATED:{}", event.getData());
                        break;
                    case CHILD_REMOVED:
                        log.info("CHILD_REMOVED:{}", event.getData());
                        break;
                    case CONNECTION_LOST:
                        log.info("CONNECTION_LOST:{}", event.getData());
                        break;
                    case CONNECTION_SUSPENDED:
                        log.info("CONNECTION_SUSPENDED:{}", event.getData());
                        break;
                    case CONNECTION_RECONNECTED:
                        log.info("CONNECTION_RECONNECTED:{}", event.getData());
                        break;
                    default:
                        break;
                }
            }
        });

        client.create().withMode(CreateMode.PERSISTENT).forPath(nodePath2);
        Thread.sleep(1000);

        client.create().withMode(CreateMode.PERSISTENT).forPath(nodePath2 + "/p1");
        Thread.sleep(1000);

        client.delete().forPath(nodePath2 + "/p1");
        Thread.sleep(1000);

        client.delete().forPath(nodePath2);
        Thread.sleep(Integer.MAX_VALUE);
    }
}
