package com.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class CreateNode {

    private static Logger log = LoggerFactory.getLogger(CreateNode.class);

    public static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper("master:2181,slave1:2181,slave2:2181", 5000, new CreateWatcher());
        log.info("Zookeeper状态：{}", zk.getState());//CONNECTING
        latch.await();
        log.info("Zookeeper状态：{}", zk.getState());//CONNECTED

        //这里不指定ACL的话会报错 KeeperErrorCode = MarshallingError for /mynode
        zk.create("/mynode", "some infos".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
    }
}
