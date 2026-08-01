package com.urbanpulse.spark.model;

import java.io.Serializable;

public class SmartMeter implements Serializable {
	private static final long serialVersionUID = 1L;

    private String meter_id;
    private String ward_id;
    private double kwh_reading;
    private double power_factor;
    private double voltage;
    private String event_time;

    public SmartMeter() {
    }

    public SmartMeter(String meter_id,
                      String ward_id,
                      double kwh_reading,
                      double power_factor,
                      double voltage,
                      String event_time) {

        this.meter_id = meter_id;
        this.ward_id = ward_id;
        this.kwh_reading = kwh_reading;
        this.power_factor = power_factor;
        this.voltage = voltage;
        this.event_time = event_time;
    }

    public String getMeter_id() {
        return meter_id;
    }

    public void setMeter_id(String meter_id) {
        this.meter_id = meter_id;
    }

    public String getWard_id() {
        return ward_id;
    }

    public void setWard_id(String ward_id) {
        this.ward_id = ward_id;
    }

    public double getKwh_reading() {
        return kwh_reading;
    }

    public void setKwh_reading(double kwh_reading) {
        this.kwh_reading = kwh_reading;
    }

    public double getPower_factor() {
        return power_factor;
    }

    public void setPower_factor(double power_factor) {
        this.power_factor = power_factor;
    }

    public double getVoltage() {
        return voltage;
    }

    public void setVoltage(double voltage) {
        this.voltage = voltage;
    }

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    @Override
    public String toString() {
        return "SmartMeter{" +
                "meter_id='" + meter_id + '\'' +
                ", ward_id='" + ward_id + '\'' +
                ", kwh_reading=" + kwh_reading +
                ", power_factor=" + power_factor +
                ", voltage=" + voltage +
                ", event_time='" + event_time + '\'' +
                '}';
    }
}