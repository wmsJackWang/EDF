package com.github.shyiko.mysql.binlog.edf.common;


import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class BatchQueue<T> {

    private final int batchSize;
    private final Consumer<List<T>> consumer;
    private final int timeoutInMs;

    private AtomicBoolean isLooping = new AtomicBoolean(false);
    private BlockingQueue<T> queue = new LinkedBlockingQueue<>(100);
    private ExecutorService executorService = Executors.newCachedThreadPool();

    private AtomicLong start = new AtomicLong(System.currentTimeMillis());

    public BatchQueue(int batchSize, int timeoutInMs, Consumer<List<T>> consumer) {
        this.batchSize = batchSize;
        this.timeoutInMs = timeoutInMs;
        this.consumer = consumer;
    }

    public BatchQueue(int batchSize, Consumer<List<T>> consumer) {
        this(batchSize, 500, consumer);
    }

    public boolean add(T t) throws Exception {
        queue.put(t);
        if (!isLooping.get()) {
            isLooping.set(true);
            startLoop();
        }
        return true;
    }

    public void completeAll() {
        while (!queue.isEmpty()) {
            drainToConsume();
        }
    }

    private void startLoop() {
        executorService.execute(new ExeThread());
    }

    private void drainToConsume() {
        List<T> drained = new ArrayList<>();
        int num = queue.drainTo(drained, batchSize);
        if(num > 0) {
            consumer.accept(drained);
            start.set(System.currentTimeMillis());
        }
    }

    private class ExeThread implements Runnable {
        @Override
        public void run() {
            start = new AtomicLong(System.currentTimeMillis());
            while(true) {
                long last = System.currentTimeMillis() - start.get() ;
                if (queue.size() >= batchSize || (!queue.isEmpty() && last > timeoutInMs)) {
                    drainToConsume();
                } else if(queue.isEmpty()) {
                    isLooping.set(false);
                    break;
                }
            }
        }
    }

    static BatchQueue<String> batchQueue = new BatchQueue<>(3, 5000, x -> {
        try {
            exe(x);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    });

    public static void main(String[] args) throws Exception {
        List<String> datas = new ArrayList<>();
        datas.add("1");
        datas.add("2");
        datas.add("3");
        datas.add("4");
        datas.add("5");
        datas.add("6");
        datas.add("7");
        datas.add("8");
        datas.add("9");
        datas.add("10");
        datas.add("11");
        datas.add("12");
        datas.add("13");
        datas.add("14");
        datas.add("15");
        datas.add("16");
        datas.add("17");
        datas.add("18");
        datas.add("19");
        datas.add("20");
        while (true) {
            if (datas.size() == 0) {
                break;
            }
            String d = datas.remove(0);
            System.out.println("add: "+d);
            batchQueue.add(d);
        }
    }

    private static void exe (List<String> o) throws InterruptedException {
        System.out.println("处理数据：" + o);
        TimeUnit.SECONDS.sleep(5);
    }
}
