package com.example;

import com.alibaba.fastjson.JSON;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateWatcher implements Watcher {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("创建ZNode节点回调事件:{}", JSON.toJSONString(watchedEvent));
        if(Event.KeeperState.SyncConnected == watchedEvent.getState()){
            CreateNode.latch.countDown();
        }
    }
}
