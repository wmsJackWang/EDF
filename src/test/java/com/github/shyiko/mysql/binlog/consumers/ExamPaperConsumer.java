package com.github.shyiko.mysql.binlog.consumers;

import com.github.shyiko.mysql.binlog.edf.consumers.EventConsumer;
import com.github.shyiko.mysql.binlog.edf.core.TableEvent;
import com.github.shyiko.mysql.binlog.edf.enums.ConsumeResult;

public class ExamPaperConsumer implements EventConsumer {
    @Override
    public ConsumeResult accept(TableEvent o) {
        System.out.println("doBiz:" + o.toString());
        return ConsumeResult.COMMITTED;
    }

    @Override
    public String getTableName() {
        return "t_exam_paper";
    }
}
