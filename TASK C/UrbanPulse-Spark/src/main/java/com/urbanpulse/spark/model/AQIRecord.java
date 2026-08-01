package com.urbanpulse.spark.model;

import java.io.Serializable;

public class AQIRecord implements Serializable {
	private static final long serialVersionUID = 1L;

    private String zone_id;
    private double aqi;
    private String event_time;

    public AQIRecord() {
    }

    public AQIRecord(String zone_id, double aqi, String event_time) {
        this.zone_id = zone_id;
        this.aqi = aqi;
        this.event_time = event_time;
    }

    public String getZone_id() {
        return zone_id;
    }

    public void setZone_id(String zone_id) {
        this.zone_id = zone_id;
    }

    public double getAqi() {
        return aqi;
    }

    public void setAqi(double aqi) {
        this.aqi = aqi;
    }

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    @Override
    public String toString() {
        return "AQIRecord{" +
                "zone_id='" + zone_id + '\'' +
                ", aqi=" + aqi +
                ", event_time='" + event_time + '\'' +
                '}';
    }
}