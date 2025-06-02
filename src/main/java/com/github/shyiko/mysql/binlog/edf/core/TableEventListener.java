package com.github.shyiko.mysql.binlog.edf.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.edf.strategy.PullStrategy;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;

import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TableEventListener implements BinaryLogClient.EventListener {

    private final Logger logger = Logger.getLogger(getClass().getName());

    TableEvent tableEvent;
    private PullStrategy pullStrategy;
    private Predicate<String> tableFilter;
    private List<String> supportTables;

    public List<String> getSupportTables() {
        return supportTables;
    }

    public TableEventListener(PullStrategy pullStrategy, List<String> supportTables) {
        if (supportTables == null || supportTables.size() == 0) {
            throw new RuntimeException("supportTables cant be empty");
        }
        this.pullStrategy = pullStrategy;
        this.supportTables = supportTables;
        tableFilter = tableName -> getSupportTables().contains(tableName);
    }

    @Override
    public void onEvent(Event event) throws InterruptedException {
//        System.out.println("curP:" + ((EventHeaderV4) event.getHeader()).getPosition() + "nextP: " + ((EventHeaderV4) event.getHeader()).getNextPosition() + JSON.toJSONString(event));
        if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
            tableEvent = new TableEvent();
            tableEvent.setTableName(JSON.parseObject(JSON.toJSONString(event.getData())).getString("table"));
            tableEvent.setMapHeader(event.getHeader());
        }

        if (event.getHeader().getEventType() == EventType.EXT_UPDATE_ROWS) {
            JSONObject data = JSON.parseObject(JSON.toJSONString(event.getData()), JSONObject.class);
            List<List<Serializable>> rows = (List<List<Serializable>>) data.get("rows");
            tableEvent.setData(rows);
            tableEvent.setSqlType(EventType.EXT_UPDATE_ROWS.name());
            tableEvent.setDataHeader(event.getHeader());
            tableEvent.setCurPosition(((EventHeaderV4) event.getHeader()).getPosition());
            tableEvent.setNextPosition(((EventHeaderV4) event.getHeader()).getNextPosition());
            tableEvent.setBinlogFile(((EventHeaderV4) event.getHeader()).getBinlogFile());
//            System.out.println("command:" + tableEvent.toString());
        }

        if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS) {
            JSONObject data = JSON.parseObject(JSON.toJSONString(event.getData()), JSONObject.class);
            List<List<Serializable>> rows = (List<List<Serializable>>) data.get("rows");
            tableEvent.setData(rows);
            tableEvent.setSqlType(EventType.EXT_WRITE_ROWS.name());
            tableEvent.setDataHeader(event.getHeader());
            tableEvent.setCurPosition(((EventHeaderV4) event.getHeader()).getPosition());
            tableEvent.setNextPosition(((EventHeaderV4) event.getHeader()).getNextPosition());
            tableEvent.setBinlogFile(((EventHeaderV4) event.getHeader()).getBinlogFile());
//            System.out.println("command:" + tableEvent.toString());
        }

        if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS || event.getHeader().getEventType() == EventType.EXT_UPDATE_ROWS) {
            if (tableFilter.test(tableEvent.getTableName())) {
                pullStrategy.pullEventData(tableEvent);
            }else {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info(String.format("tableName:%s not support", tableEvent.getTableName()));
                }
            }
        }
    }
}
