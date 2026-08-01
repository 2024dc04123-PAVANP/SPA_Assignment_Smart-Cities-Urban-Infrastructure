package com.urbanpulse.detection;

/**
 * Apache Flink application to detect AQI emergency incidents.
 *
 * Logic:
 * - Reads air quality events.
 * - Detects AQI greater than 300.
 * - Generates IncidentAlert.
 *
 * Assignment: UrbanPulse Q9(a)
 */

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.urbanpulse.model.AirQuality;
import com.urbanpulse.model.IncidentAlert;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AQIDetection {

    public static void main(String[] args) throws Exception {

        // Step 1: Create Flink Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Create Sample Data
        DataStream<AirQuality> airQualityStream = env

                .fromElements(

                        new AirQuality("AQ101", "Central", 120, System.currentTimeMillis()),

                        new AirQuality("AQ102", "West", 340, System.currentTimeMillis()),

                        new AirQuality("AQ103", "North", 250, System.currentTimeMillis()),

                        new AirQuality("AQ104", "South", 360, System.currentTimeMillis()),

                        new AirQuality("AQ105", "East", 180, System.currentTimeMillis())

                )

                .assignTimestampsAndWatermarks(

                        WatermarkStrategy

                                .<AirQuality>forBoundedOutOfOrderness(Duration.ofMinutes(2))

                                .withTimestampAssigner(

                                        new SerializableTimestampAssigner<AirQuality>() {

                                            @Override
                                            public long extractTimestamp(AirQuality element, long recordTimestamp) {

                                                return element.getTimestamp();

                                            }

                                        }

                                )

                );

        // Step 3: Filter AQI > 300 and create IncidentAlert
        DataStream<IncidentAlert> alerts = airQualityStream

                .filter(sensor -> sensor.getAqi() > 300)

                .map(sensor -> new IncidentAlert(
                        "AQI_EMERGENCY",
                        sensor.getZone(),
                        "Hazardous AQI detected at Sensor "
                                + sensor.getSensorId()
                                + " (AQI = "
                                + sensor.getAqi()
                                + ")",
                        sensor.getTimestamp()
                ));

        // Step 4: Print Alerts
        alerts.print();

        // Step 5: Execute Flink Job
        env.execute("UrbanPulse AQI Emergency Detection");
    }
}