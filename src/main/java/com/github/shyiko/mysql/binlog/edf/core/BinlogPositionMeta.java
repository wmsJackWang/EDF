package com.github.shyiko.mysql.binlog.edf.core;

public class BinlogPositionMeta {
    private String binlogFilename;
    private long binlogPosition = 4;

    public BinlogPositionMeta(){

    }

    public BinlogPositionMeta(String binlogFilename, long binlogPosition){
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }
}
