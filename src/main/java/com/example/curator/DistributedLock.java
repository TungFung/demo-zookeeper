package com.example.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private static Logger log = LoggerFactory.getLogger(DistributedLock.class);

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

        InterProcessMutex lock = new InterProcessMutex(client, "/test_lock");
        CountDownLatch latch = new CountDownLatch(1);//相当于semaphore控制,所有线程创建好了再一起跑

        for(int i = 0; i < 30; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        latch.await();//等所有线程创建好了才一起开始竞争
                        lock.acquire();//竞争分布式锁,这里如果不加锁控制，那么同一毫秒中将会有相同的订单号
                    }catch (Exception e){}

                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
                    String orderNo = sdf.format(new Date());
                    log.info("生成的订单号：{}", orderNo);

                    try {
                        lock.release();//释放锁
                    }catch (Exception e){}
                }
            }).start();
        }

        latch.countDown();
    }
}
