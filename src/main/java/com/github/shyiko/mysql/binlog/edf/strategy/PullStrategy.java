package com.github.shyiko.mysql.binlog.edf.strategy;

import com.github.shyiko.mysql.binlog.edf.core.TableEvent;

public interface PullStrategy {

    void pullEventData(TableEvent tableEvent) throws InterruptedException;
}
