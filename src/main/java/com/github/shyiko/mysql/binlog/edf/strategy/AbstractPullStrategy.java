package com.github.shyiko.mysql.binlog.edf.strategy;

import com.github.shyiko.mysql.binlog.edf.consumers.EventConsumer;
import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractPullStrategy implements PullStrategy {

    private ConcurrentHashMap<String, List<EventConsumer>> consumerConcurrentHashMap = new ConcurrentHashMap<>();

    // 添加消费者
    public void addConsumer(EventConsumer ...consumers) {
        for (EventConsumer consumer:consumers){
            List<EventConsumer> eventConsumers = consumerConcurrentHashMap.get(consumer.getTableName());
            if (eventConsumers == null){
                eventConsumers = new ArrayList<>();
            }
            eventConsumers.add(consumer);
            consumerConcurrentHashMap.put(consumer.getTableName(), eventConsumers);
        }
    }

    // 通知消费者
    public ConsumeResult notifyConsumer(TableEvent tableEvent) {
        AtomicReference<ConsumeResult> consumeResult = new AtomicReference<>();
        List<EventConsumer> consumers = consumerConcurrentHashMap.get(tableEvent.getTableName());
        if (consumers == null || consumers.isEmpty()) {
            throw new IllegalStateException("No consumer found for table " + tableEvent.getTableName());
        }
        Optional.ofNullable(consumers).orElseGet(ArrayList::new)
            .forEach(consumer -> consumeResult.set(consumer.accept(tableEvent)));

        return consumeResult.get();
    }

    public void savePosition(TableEvent tableEvent) {
        System.out.println("saveNextPosition[ fileName:" + tableEvent.getBinlogFile() + ", position:" + tableEvent.getNextPosition() + " ]");
    }

}
