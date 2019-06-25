package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master选举的实现原理
 * 利用写入临时有序节点，写入最早的成为Master
 * 当Master执行完后会话断开，临时节点销毁，
 * 其他节点监听到节点变化，当看到自己的节点序号成为当前最小的，就能接上成为新的Master
 * 这是curator实现的原理
 *
 * 也可以利用zk节点存在不能重复创建的特性，多台机器同时创建同名的节点，创建成功的那台作为master
 */
public class MasterSelect {

    private static Logger log = LoggerFactory.getLogger(MasterSelect.class);

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

        LeaderSelector selector = new LeaderSelector(client, "/master_node", new LeaderSelectorListenerAdapter() {

            /**
             * 当竞争到Master时就会自动调用该方法，一旦执行完就会立即放弃Master权利
             * 像这里/businessBase/master_node下面会不断创建如下节点
             * _c_e38cd341-aa9b-4dc4-be00-c10b164e1e3b-lock-0000000003
             * _c_5411e9da-10d0-43a3-b8cb-6768ca147c38-lock-0000000004
             */
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                log.info("成为Master角色");
                Thread.sleep(3000);
                log.info("完成Master操作，释放Master权利");
            }
        });
        selector.autoRequeue();//执行完后，又重新加入进去，再继续参选
        selector.start();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
