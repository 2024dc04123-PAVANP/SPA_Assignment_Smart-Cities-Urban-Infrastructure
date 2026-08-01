package com.urbanpulse.model;

public class AirQuality {

    // Sensor ID
    private String sensorId;

    // Zone name
    private String zone;

    // AQI Value
    private int aqi;

    // Event timestamp
    private long timestamp;

    // Default Constructor
    public AirQuality() {
    }

    // Parameterized Constructor
    public AirQuality(String sensorId, String zone, int aqi, long timestamp) {
        this.sensorId = sensorId;
        this.zone = zone;
        this.aqi = aqi;
        this.timestamp = timestamp;
    }

    // Getter
    public String getSensorId() {
        return sensorId;
    }

    // Setter
    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    // Getter
    public String getZone() {
        return zone;
    }

    // Setter
    public void setZone(String zone) {
        this.zone = zone;
    }

    // Getter
    public int getAqi() {
        return aqi;
    }

    // Setter
    public void setAqi(int aqi) {
        this.aqi = aqi;
    }

    // Getter
    public long getTimestamp() {
        return timestamp;
    }

    // Setter
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AirQuality{" +
                "sensorId='" + sensorId + '\'' +
                ", zone='" + zone + '\'' +
                ", aqi=" + aqi +
                ", timestamp=" + timestamp +
                '}';
    }
}