package com.github.shyiko.mysql.binlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.shyiko.mysql.binlog.edf.consumers.ExamPaperConsumer;
import com.github.shyiko.mysql.binlog.edf.core.TableEventListener;
import com.github.shyiko.mysql.binlog.edf.strategy.BatchEventPullStrategy;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
        client.registerEventListener(new TableEventListener(new BatchEventPullStrategy(10,  new ExamPaperConsumer())));
        client.connect();
    }

}
