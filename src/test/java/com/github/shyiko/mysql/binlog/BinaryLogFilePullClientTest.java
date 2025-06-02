package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.consumers.ExamPaperConsumer;
import com.github.shyiko.mysql.binlog.consumers.JdkFileConsumer;
import com.github.shyiko.mysql.binlog.consumers.KnowledgeConsumer;
import com.github.shyiko.mysql.binlog.consumers.UserEventLogConsumer;
import com.github.shyiko.mysql.binlog.edf.core.HeartbeatConfig;
import com.github.shyiko.mysql.binlog.edf.core.TableEventListener;
import com.github.shyiko.mysql.binlog.edf.strategy.BatchEventPullStrategy;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.IOException;

public class BinaryLogFilePullClientTest {

    @Test
    public void testEventListenersManagement() throws IOException {
        BinaryLogClient client = new BinaryLogClient("bittechblog.com", 3306, "repl", "p4ssword");
        client.setSSLMode(SSLMode.DISABLED);
        EventDeserializer eventDeserializer = new EventDeserializer();
//        eventDeserializer.setCompatibilityMode(
////                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
//                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
//        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new TableEventListener(new BatchEventPullStrategy(100,10, 10000, new ExamPaperConsumer(),  new UserEventLogConsumer(),  new JdkFileConsumer()), Lists.newArrayList("t_text_content")));
        client.connect();
    }

    public static void main(String[] args) throws IOException, InterruptedException {


//        BinlogPositionMeta positionMeta = new BinlogPositionMeta("mysql-bin.000001", 7720810);
//        BinlogPositionMeta positionMeta = new BinlogPositionMeta("mysql-bin.000001", 7720810);
        HeartbeatConfig heartbeatConfig = new HeartbeatConfig(0);
        BinaryLogPullClient client = new BinaryLogPullClient("bittechblog.com", 3306, "repl", "p4ssword", null, heartbeatConfig);
        EventDeserializer eventDeserializer = new EventDeserializer();
//        eventDeserializer.setCompatibilityMode(
////                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
//                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
//        );
        BatchEventPullStrategy eventPullStrategy = new BatchEventPullStrategy(4, 2, 10000,  new ExamPaperConsumer(),
            new UserEventLogConsumer(),  new JdkFileConsumer(),  new KnowledgeConsumer());
        TableEventListener tableEventListener = new TableEventListener(eventPullStrategy,
            Lists.newArrayList("t_exam_paper", "t_user_event_log", "jdk_folder_file", "jdk_folder_file", "konwledge_store"));
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(tableEventListener);
        client.connect();

    }

}
