package com.urbanpulse.spark.model;

import java.io.Serializable;

public class ZoneProfile implements Serializable {
	private static final long serialVersionUID = 1L;

    private String zone_id;
    private String zone_name;
    private long population;
    private int num_schools;

    public ZoneProfile() {
    }

    public ZoneProfile(String zone_id,
                       String zone_name,
                       long population,
                       int num_schools) {

        this.zone_id = zone_id;
        this.zone_name = zone_name;
        this.population = population;
        this.num_schools = num_schools;
    }

    public String getZone_id() {
        return zone_id;
    }

    public void setZone_id(String zone_id) {
        this.zone_id = zone_id;
    }

    public String getZone_name() {
        return zone_name;
    }

    public void setZone_name(String zone_name) {
        this.zone_name = zone_name;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public int getNum_schools() {
        return num_schools;
    }

    public void setNum_schools(int num_schools) {
        this.num_schools = num_schools;
    }

    @Override
    public String toString() {
        return "ZoneProfile{" +
                "zone_id='" + zone_id + '\'' +
                ", zone_name='" + zone_name + '\'' +
                ", population=" + population +
                ", num_schools=" + num_schools +
                '}';
    }
}