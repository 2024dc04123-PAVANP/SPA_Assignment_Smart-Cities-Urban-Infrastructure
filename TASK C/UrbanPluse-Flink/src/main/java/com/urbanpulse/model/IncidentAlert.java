package com.urbanpulse.model;

public class IncidentAlert {

    private String incidentType;
    private String location;
    private String message;
    private long timestamp;

    public IncidentAlert() {
    }

    public IncidentAlert(String incidentType, String location, String message, long timestamp) {
        this.incidentType = incidentType;
        this.location = location;
        this.message = message;
        this.timestamp = timestamp;
    }

    public String getIncidentType() {
        return incidentType;
    }

    public void setIncidentType(String incidentType) {
        this.incidentType = incidentType;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "IncidentAlert{" +
                "incidentType='" + incidentType + '\'' +
                ", location='" + location + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}