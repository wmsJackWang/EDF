package com.github.shyiko.mysql.binlog.edf.strategy;

import com.alibaba.fastjson.JSON;
import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class SingleEventPullStrategy extends AbstractPullStrategy {

    CountDownLatch latch;

    @Override
    public void pullEventData(TableEvent tableEvent) {
        latch.countDown();
        ConsumeResult consumeResult = notifyConsumer(tableEvent);
        if (Objects.nonNull(consumeResult) && consumeResult == ConsumeResult.COMMITTED) {
            savePosition(tableEvent);
        }else {
            throw new RuntimeException("event data consumer failed, consume event: " + JSON.toJSONString(consumeResult));
        }
    }

}
