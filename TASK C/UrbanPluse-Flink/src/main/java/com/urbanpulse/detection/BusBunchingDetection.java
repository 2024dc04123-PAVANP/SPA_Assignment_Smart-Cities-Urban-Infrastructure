package com.urbanpulse.detection;

/**
 * Detects bus bunching incidents.
 *
 * Logic:
 * - Group buses by Route ID
 * - Compare GPS coordinates
 * - Generate alert when buses are within 200 metres.
 *
 * Note:
 * A production implementation would use an Event-Time Timer
 * to verify the buses remain together for more than 5 minutes.
 *
 * Assignment: UrbanPulse Q9(c)
 */


import com.urbanpulse.model.BusEvent;
import com.urbanpulse.model.IncidentAlert;
// import com.urbanpulse.sink.KafkaIncidentSink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BusBunchingDetection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        DataStream<BusEvent> busStream = env.fromElements(

                new BusEvent("B101","R101",19.0760,72.8777,System.currentTimeMillis()),
                new BusEvent("B102","R101",19.0762,72.8778,System.currentTimeMillis()),

                new BusEvent("B201","R102",19.2000,72.9000,System.currentTimeMillis()),
                new BusEvent("B202","R102",19.5000,73.1000,System.currentTimeMillis())

        );

        DataStream<IncidentAlert> alerts = busStream

                .keyBy(BusEvent::getRouteId)

                .process(new KeyedProcessFunction<String, BusEvent, IncidentAlert>() {

                    private MapState<String, BusEvent> busState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        MapStateDescriptor<String, BusEvent> descriptor =
                                new MapStateDescriptor<>(
                                        "busState",
                                        String.class,
                                        BusEvent.class);

                        busState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(
                            BusEvent currentBus,
                            Context ctx,
                            Collector<IncidentAlert> out) throws Exception {

                        for (String busId : busState.keys()) {

                            BusEvent previousBus = busState.get(busId);

                            if (!previousBus.getBusId().equals(currentBus.getBusId())) {

                                double distance = calculateDistance(
                                        previousBus.getLatitude(),
                                        previousBus.getLongitude(),
                                        currentBus.getLatitude(),
                                        currentBus.getLongitude());

                                if (distance <= 200) {

                                    out.collect(new IncidentAlert(
                                            "BUS_BUNCHING",
                                            currentBus.getRouteId(),
                                            "Bus " + previousBus.getBusId()
                                                    + " and Bus "
                                                    + currentBus.getBusId()
                                                    + " are within "
                                                    + String.format("%.2f", distance)
                                                    + " metres.",
                                            currentBus.getTimestamp()
                                    ));
                                }
                            }
                        }

                        busState.put(currentBus.getBusId(), currentBus);
                    }

                    private double calculateDistance(
                            double lat1,
                            double lon1,
                            double lat2,
                            double lon2) {

                        final int R = 6371000;

                        double dLat = Math.toRadians(lat2 - lat1);
                        double dLon = Math.toRadians(lon2 - lon1);

                        double a =
                                Math.sin(dLat / 2) * Math.sin(dLat / 2)
                                        + Math.cos(Math.toRadians(lat1))
                                        * Math.cos(Math.toRadians(lat2))
                                        * Math.sin(dLon / 2)
                                        * Math.sin(dLon / 2);

                        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

                        return R * c;
                    }

                });

        alerts.print();

        //alerts.sinkTo(KafkaIncidentSink.create());
        env.execute("Bus Bunching Detection");
    }
}