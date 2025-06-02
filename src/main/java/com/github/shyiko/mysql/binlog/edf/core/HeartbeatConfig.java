package com.github.shyiko.mysql.binlog.edf.core;

public class HeartbeatConfig {
    private long heartbeatInterval;

    public HeartbeatConfig(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }
}
