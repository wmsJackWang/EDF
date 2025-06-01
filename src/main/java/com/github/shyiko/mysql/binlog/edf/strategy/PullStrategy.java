package com.github.shyiko.mysql.binlog.edf.strategy;

import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;

import java.util.concurrent.CountDownLatch;

public interface PullStrategy {

    void pullEventData(TableEvent tableEvent) throws InterruptedException;
}
