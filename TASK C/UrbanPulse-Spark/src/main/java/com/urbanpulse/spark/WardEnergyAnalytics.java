package com.urbanpulse.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * Assignment Task C - Problem Statement 11. 
 *
 * Input Kafka topic:
 *   urbanpulse.smart_meters
 *
 * Output Kafka topic:
 *   ward_energy_summary
 *
 */
public class WardEnergyAnalytics {

    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "urbanpulse.smart_meters";
    private static final String OUTPUT_TOPIC = "ward_energy_summary";

    private static final String PARQUET_PATH =
            "output/ward_energy_summary";

    private static final String CHECKPOINT_ROOT =
            "checkpoints/ward_energy_summary";

    public static void main(String[] args)
            throws TimeoutException, StreamingQueryException {

        String kafkaBootstrapServers =
                args.length > 0 ? args[0] : KAFKA_SERVERS;

        String parquetOutputPath =
                args.length > 1 ? args[1] : PARQUET_PATH;

        String checkpointRoot =
                args.length > 2 ? args[2] : CHECKPOINT_ROOT;

        SparkSession spark = SparkSession.builder()
                .appName("UrbanPulse Ward Energy Analytics")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println(" UrbanPulse Ward Energy Analytics Started");
        System.out.println(" Input topic   : " + INPUT_TOPIC);
        System.out.println(" Output topic  : " + OUTPUT_TOPIC);
        System.out.println(" Parquet path  : " + parquetOutputPath);


        StructType smartMeterSchema = new StructType()
                .add("meter_id", DataTypes.StringType)
                .add("ward_id", DataTypes.StringType)
                .add("kwh_reading", DataTypes.DoubleType)
                .add("power_factor", DataTypes.DoubleType)
                .add("voltage", DataTypes.DoubleType)
                .add("event_time", DataTypes.StringType);

        
        // Reading the smart-meter event stream from Kafka.
        
        Dataset<Row> kafkaInput = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", INPUT_TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();


        Dataset<Row> smartMeterEvents = kafkaInput
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(
                        from_json(
                                col("json_value"),
                                smartMeterSchema
                        ).alias("meter")
                )
                .select("meter.*")
                .withColumn(
                        "event_timestamp",
                        to_timestamp(col("event_time"))
                )
                .filter(
                        col("ward_id").isNotNull()
                                .and(col("event_timestamp").isNotNull())
                                .and(col("kwh_reading").isNotNull())
                                .and(col("power_factor").isNotNull())
                                .and(col("voltage").isNotNull())
                );

        
        // The watermark is calculated as the maximum observed event time minus five minutes. 
        // Events older than this watermark may be treated as late and dropped after the relevant window is finalized.   

        Dataset<Row> wardEnergySummary = smartMeterEvents
                .withWatermark("event_timestamp", "5 minutes")
                .groupBy(
                        col("ward_id"),
                        window(
                                col("event_timestamp"),
                                "15 minutes"
                        )
                )
                .agg(
                        sum("kwh_reading")
                                .alias("total_kwh_consumed"),

                        avg("power_factor")
                                .alias("avg_power_factor"),

                        max("voltage")
                                .alias("peak_voltage")
                )
                .select(
                        col("ward_id"),
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),

                        round(
                                col("total_kwh_consumed"),
                                3
                        ).alias("total_kwh_consumed"),

                        round(
                                col("avg_power_factor"),
                                4
                        ).alias("avg_power_factor"),

                        round(
                                col("peak_voltage"),
                                3
                        ).alias("peak_voltage"),


                        to_date(
                                col("window.start")
                        ).alias("date")
                );

        // The Kafka key uniquely identifies a ward and 15-minute window.

        Dataset<Row> kafkaOutput = wardEnergySummary.select(
                concat_ws(
                        "_",
                        col("ward_id"),
                        date_format(
                                col("window_start"),
                                "yyyyMMddHHmm"
                        )
                ).cast("string").alias("key"),

                to_json(
                        struct(
                                col("ward_id"),
                                col("window_start"),
                                col("window_end"),
                                col("total_kwh_consumed"),
                                col("avg_power_factor"),
                                col("peak_voltage"),
                                col("date")
                        )
                ).alias("value")
        );

        // Sink 1: Write completed ward/window summaries to Kafka.

        StreamingQuery kafkaQuery = kafkaOutput.writeStream()
                .queryName("ward-energy-kafka-sink")
                .format("kafka")
                .outputMode("append")
                .option(
                        "kafka.bootstrap.servers",
                        kafkaBootstrapServers
                )
                .option("topic", OUTPUT_TOPIC)
                .option(
                        "checkpointLocation",
                        checkpointRoot + "/kafka"
                )
                .start();

        
        // Sink 2: Write the same completed summaries to Parquet.

        StreamingQuery parquetQuery = wardEnergySummary.writeStream()
                .queryName("ward-energy-parquet-sink")
                .format("parquet")
                .outputMode("append")
                .option("path", parquetOutputPath)
                .option(
                        "checkpointLocation",
                        checkpointRoot + "/parquet"
                )
                .partitionBy("ward_id", "date")
                .start();

        
        // The application is running while both streaming queries are executing.
        
        try {
            spark.streams().awaitAnyTermination();
        } finally {
            if (kafkaQuery.isActive()) {
                kafkaQuery.stop();
            }

            if (parquetQuery.isActive()) {
                parquetQuery.stop();
            }

            spark.stop();
        }
    }
}