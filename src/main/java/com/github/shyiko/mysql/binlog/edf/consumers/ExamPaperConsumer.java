package com.github.shyiko.mysql.binlog.edf.consumers;

import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

import java.util.function.Consumer;

public class ExamPaperConsumer implements EventConsumer {
    @Override
    public ConsumeResult accept(TableEvent o) {
        return ConsumeResult.COMMITTED;
    }

    @Override
    public String getTableName() {
        return "t_exam_paper";
    }
}
