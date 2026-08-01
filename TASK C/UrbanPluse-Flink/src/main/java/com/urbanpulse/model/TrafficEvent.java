package com.urbanpulse.model;

public class TrafficEvent {

    private String junctionId;
    private String zone;
    private int averageWait;
    private long timestamp;

    public TrafficEvent() {
    }

    public TrafficEvent(String junctionId, String zone, int averageWait, long timestamp) {
        this.junctionId = junctionId;
        this.zone = zone;
        this.averageWait = averageWait;
        this.timestamp = timestamp;
    }

    public String getJunctionId() {
        return junctionId;
    }

    public void setJunctionId(String junctionId) {
        this.junctionId = junctionId;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public int getAverageWait() {
        return averageWait;
    }

    public void setAverageWait(int averageWait) {
        this.averageWait = averageWait;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TrafficEvent{" +
                "junctionId='" + junctionId + '\'' +
                ", zone='" + zone + '\'' +
                ", averageWait=" + averageWait +
                ", timestamp=" + timestamp +
                '}';
    }
}