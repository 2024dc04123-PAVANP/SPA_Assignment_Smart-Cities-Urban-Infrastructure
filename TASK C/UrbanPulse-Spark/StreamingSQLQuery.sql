import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class UrbanPulseHealthAdvisory {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("UrbanPulse Health Advisory")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // -------------------------------------------------------
        // 1. Read static zone_profile table
        // -------------------------------------------------------
        Dataset<Row> zoneProfile = spark.read()
                .format("delta")
                .load("/data/zone_profile");

        zoneProfile.createOrReplaceTempView("zone_profile");


        // -------------------------------------------------------
        // 2. Read AQI stream from Kafka
        // -------------------------------------------------------
        Dataset<Row> kafkaInput = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "urbanpulse.air_quality")
                .option("startingOffsets", "latest")
                .load();


        // -------------------------------------------------------
        // 3. Parse incoming JSON
        // Example JSON:
        // {
        //   "zone_id":"Z01",
        //   "aqi":180,
        //   "event_time":"2026-07-26 10:05:00"
        // }
        // -------------------------------------------------------

        String schema =
                "zone_id STRING, " +
                "aqi DOUBLE, " +
                "event_time TIMESTAMP";

        Dataset<Row> aqiStream = kafkaInput
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(
                        from_json(
                                col("json_value"),
                                schema
                        ).alias("data")
                )
                .select("data.*");


        // -------------------------------------------------------
        // 4. Apply watermark for late events
        // -------------------------------------------------------
        Dataset<Row> aqiWithWatermark = aqiStream
                .withWatermark("event_time", "2 minutes");

        aqiWithWatermark.createOrReplaceTempView(
                "aqi_with_watermark"
        );


        // -------------------------------------------------------
        // 5. Streaming SQL
        //
        // 10-minute rolling average
        // sliding every 1 minute
        //
        // Join static zone_profile
        //
        // Filter rolling_avg_aqi > 150
        // -------------------------------------------------------

        Dataset<Row> healthAdvisories = spark.sql(
                """
                SELECT
                    a.zone_id,
                    z.zone_name,
                    z.population,
                    z.num_schools,
                    window(
                        a.event_time,
                        '10 minutes',
                        '1 minute'
                    ).start AS window_start,
                    window(
                        a.event_time,
                        '10 minutes',
                        '1 minute'
                    ).end AS window_end,
                    AVG(a.aqi) AS rolling_avg_aqi
                FROM aqi_with_watermark a
                JOIN zone_profile z
                    ON a.zone_id = z.zone_id
                GROUP BY
                    a.zone_id,
                    z.zone_name,
                    z.population,
                    z.num_schools,
                    window(
                        a.event_time,
                        '10 minutes',
                        '1 minute'
                    )
                HAVING AVG(a.aqi) > 150
                """
        );


        // -------------------------------------------------------
        // 6. Convert output to Kafka key/value format
        // -------------------------------------------------------

        Dataset<Row> kafkaOutput = healthAdvisories
                .select(
                        col("zone_id")
                                .cast("string")
                                .alias("key"),

                        to_json(
                                struct(
                                        col("zone_id"),
                                        col("zone_name"),
                                        col("population"),
                                        col("num_schools"),
                                        col("window_start"),
                                        col("window_end"),
                                        col("rolling_avg_aqi")
                                )
                        ).alias("value")
                );


        // -------------------------------------------------------
        // 7. Write health advisories to Kafka
        //
        // Required output mode: UPDATE
        // -------------------------------------------------------

        StreamingQuery query = kafkaOutput
                .writeStream()
                .format("kafka")
                .option(
                        "kafka.bootstrap.servers",
                        "localhost:9092"
                )
                .option(
                        "topic",
                        "urbanpulse.health_advisories"
                )
                .option(
                        "checkpointLocation",
                        "/tmp/checkpoints/health_advisories"
                )
                .outputMode("update")
                .start();


        // Keep application running
        query.awaitTermination();
    }
}