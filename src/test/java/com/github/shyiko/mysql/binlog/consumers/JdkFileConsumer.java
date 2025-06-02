package com.github.shyiko.mysql.binlog.consumers;

import com.github.shyiko.mysql.binlog.edf.consumers.EventConsumer;
import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

import java.util.concurrent.TimeUnit;

public class JdkFileConsumer implements EventConsumer {
    @Override
    public ConsumeResult accept(TableEvent o) {
        System.out.println("doBiz:" + o.toString());
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return ConsumeResult.COMMITTED;
    }

    @Override
    public String getTableName() {
        return "jdk_folder_file";
    }
}
