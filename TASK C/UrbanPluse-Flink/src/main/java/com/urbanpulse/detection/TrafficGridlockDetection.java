package com.urbanpulse.detection;

/**
 * Detects traffic gridlock using Flink ValueState.
 *
 * Gridlock Condition:
 * Average wait time >180 seconds
 * for three consecutive signal cycles.
 *
 * Assignment: UrbanPulse Q9(b)
 */

import com.urbanpulse.model.TrafficEvent;
import com.urbanpulse.model.IncidentAlert;
// import com.urbanpulse.sink.KafkaIncidentSink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import org.apache.flink.util.Collector;

public class TrafficGridlockDetection {

    public static void main(String[] args) throws Exception {

        // Create Flink Environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        // Sample Traffic Events
        DataStream<TrafficEvent> trafficStream = env.fromElements(

                new TrafficEvent("J101", "Central", 190, System.currentTimeMillis()),
                new TrafficEvent("J101", "Central", 200, System.currentTimeMillis()),
                new TrafficEvent("J101", "Central", 210, System.currentTimeMillis()),

                new TrafficEvent("J102", "East", 100, System.currentTimeMillis()),
                new TrafficEvent("J102", "East", 110, System.currentTimeMillis()),

                new TrafficEvent("J103", "West", 220, System.currentTimeMillis()),
                new TrafficEvent("J103", "West", 230, System.currentTimeMillis()),
                new TrafficEvent("J103", "West", 240, System.currentTimeMillis())

        );

        DataStream<IncidentAlert> alerts = trafficStream

                .keyBy(TrafficEvent::getJunctionId)

                .process(new KeyedProcessFunction<String, TrafficEvent, IncidentAlert>() {

                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) {

                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<>("countState", Integer.class);

                        countState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(
                            TrafficEvent event,
                            Context context,
                            Collector<IncidentAlert> out) throws Exception {

                        Integer count = countState.value();

                        if (count == null) {
                            count = 0;
                        }

                        if (event.getAverageWait() > 180) {

                            count++;

                            countState.update(count);

                            if (count >= 3) {

                                out.collect(new IncidentAlert(

                                        "TRAFFIC_GRIDLOCK",

                                        event.getZone(),

                                        "Gridlock detected at Junction "
                                                + event.getJunctionId(),

                                        event.getTimestamp()

                                ));

                                countState.clear();
                            }

                        } else {

                            countState.clear();

                        }

                    }

                });

        alerts.print();

        //alerts.sinkTo(KafkaIncidentSink.create());
        env.execute("Traffic Gridlock Detection");
    }
}