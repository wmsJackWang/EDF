package com.github.shyiko.mysql.binlog.edf.consumers;

import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

public interface EventConsumer {

    String getTableName();

    default boolean filter(TableEvent event){
        if (event.getTableName().equals(getTableName())){
            return true;
        }
        return false;
    }

    ConsumeResult accept(TableEvent event);
}
