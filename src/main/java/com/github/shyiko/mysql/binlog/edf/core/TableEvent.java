package com.github.shyiko.mysql.binlog.edf.core;

import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

import java.io.Serializable;
import java.util.List;

/*
 * 库表事件
 */
public class TableEvent {
    String sqlType;
    String tableName;
    List<List<Serializable>> data;
    private EventHeader mapHeader;
    private EventHeader dataHeader;
    private Long nextPosition;
    private Long curPosition;

    public String toString(){
        return "TableEvent [sqlType=" + sqlType + ", tableName=" + tableName +  ", nextPosition=" + nextPosition +  ", curPosition=" + curPosition +  ", dataSize=" + data.size() + "]";
    }

    public void setCurPosition(Long curPosition) {
        this.curPosition = curPosition;
    }

    public Long getNextPosition() {
        return nextPosition;
    }

    public Long getCurPosition() {
        return curPosition;
    }

    public void setNextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<List<Serializable>> getData() {
        return data;
    }

    public void setData(List<List<Serializable>> data) {
        this.data = data;
    }

    public void setDataHeader(EventHeader dataHeader) {
        this.dataHeader = dataHeader;
    }

    public EventHeader getDataHeader() {
        return dataHeader;
    }

    public void setMapHeader(EventHeader mapHeader) {
        this.mapHeader = mapHeader;
    }

    public EventHeader getMapHeader() {
        return mapHeader;
    }

    public boolean oneOperation(){

        return ((EventHeaderV4) mapHeader).getNextPosition() == ((EventHeaderV4) dataHeader).getPosition();
    }
}
