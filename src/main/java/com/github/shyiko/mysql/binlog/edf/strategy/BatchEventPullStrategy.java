package com.github.shyiko.mysql.binlog.edf.strategy;

import com.alibaba.fastjson.JSON;
import com.github.shyiko.mysql.binlog.edf.common.BatchQueue;
import com.github.shyiko.mysql.binlog.edf.consumers.EventConsumer;
import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;
import com.github.shyiko.mysql.binlog.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchEventPullStrategy extends AbstractPullStrategy {

    private final Integer batchSize;
    CountDownLatch latch;
    BatchQueue<TableEvent> batchQueue;

    ExecutorService executor;

    public BatchEventPullStrategy(Integer batchSize, EventConsumer ...consumer) {
        this.batchSize = batchSize;
        this.addConsumer(consumer);
        executor = new ThreadPoolExecutor(1, 20,
            0L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            r -> new Thread(r, "MutiEventPullStrategy")
            , new ThreadPoolExecutor.CallerRunsPolicy());
        batchQueue = new BatchQueue<>(this.batchSize, 500, events -> {
            try {
                batchHandle(events);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void batchHandle(List<TableEvent> events) throws Exception {
        latch = new CountDownLatch(events.size());
        events.forEach(event -> {
            executor.execute(() -> {
                ConsumeResult consumeResult = notifyConsumer(event);
                if (consumeResult == ConsumeResult.COMMITTED){
                    latch.countDown();
                    return;
                }
                throw new RuntimeException(String.format("consume result:%s, event:%s", JSON.toJSONString(consumeResult), JSON.toJSONString(event)));
            });
        });
        latch.await();
        savePosition(events.get(events.size() - 1));
    }
    @Override
    public void pullEventData(TableEvent tableEvent) {

        try {
            batchQueue.add(tableEvent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static class TableEventBuffer{
        public TableEventBuffer() {
            events = new ArrayList<>();
        }
        List<TableEvent> events;
        TableEvent newEvent;//最新事件
        TableEvent oldEvent;//最久事件
        public void clear() {
            events.clear();
            newEvent = null;
            oldEvent = null;
        }
    }
}
