package com.urbanpulse.spark;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class UrbanPulseHealthAdvisory {

    public static void main(String[] args) throws Exception {

        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("UrbanPulse Health Advisory")
                .master("local[*]")
                .getOrCreate();

        // Reduce console logs
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=======================================");
        System.out.println(" UrbanPulse Health Advisory Started ");
        System.out.println("=======================================");

        // AQI JSON Schema
        StructType airQualitySchema = new StructType()
                .add("zone_id", DataTypes.StringType)
                .add("aqi", DataTypes.DoubleType)
                .add("event_time", DataTypes.TimestampType);
        
        java.io.File file = new java.io.File("src/main/resources/zone_profile.csv");
        System.out.println("Reading file from:");
        System.out.println(file.getAbsolutePath());
        System.out.println("File exists: " + file.exists());

        // Read Zone Profile CSV
        Dataset<Row> zone_profile = spark.read()
                .format("csv")
                .option("header", true)
                .option("delimiter", ",")
                .option("inferSchema", true)
                .load("src/main/resources/zone_profile.csv");
        
		
		  // combine the column
        Dataset<Row> formattedOutput = zone_profile.select(
		  concat_ws(" ", zone_profile.col("zone_id"), zone_profile.col("zone_name"),
				zone_profile.col("population"), zone_profile.col("num_schools") ).as("Output")
		  );
		 
        

        System.out.println("\nZone Profile Data:");

        zone_profile.printSchema();
        zone_profile.show(false);
        

        System.out.println("Zone Profile Loaded Successfully.");
        
     // Read Zone Profile CSV
        Dataset<Row> zoneProfile = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("src/main/resources/zone_profile.csv");

        System.out.println("\nZone Profile Data:");
        zoneProfile.printSchema();
        zoneProfile.show(false);

        System.out.println("Zone Profile Loaded Successfully.");


        // ===========================
        // Read AQI JSON
        // ===========================

        Dataset<Row> aqiData = spark.read()
                .schema(airQualitySchema)
                .json("src/main/resources/aqi_stream.json");

        System.out.println("\nAQI Stream:");

        aqiData.printSchema();
        aqiData.show(false);

        System.out.println("AQI Stream Loaded Successfully.");
        
        System.out.println("jpining CSV and JSON dataset");
        
        Dataset<Row> joinedData = zoneProfile.join(
                aqiData,
                zoneProfile.col("zone_id")
                        .equalTo(aqiData.col("zone_id")),
                "inner"
        );

        System.out.println("\nJoined Data:");

        joinedData.show(false);
        
        System.out.println("AQI Category // ");
        
        Dataset<Row> advisory = joinedData.withColumn(
                "AQI_Category",
                when(col("aqi").lt(51), "Good")
                .when(col("aqi").lt(101), "Moderate")
                .when(col("aqi").lt(201), "Unhealthy")
                .when(col("aqi").lt(301), "Very Unhealthy")
                .otherwise("Hazardous")
        );

        System.out.println("\nAQI Categories:");

        advisory.show(false);
       
        System.out.println("Health advisory //");
        
        advisory = advisory.withColumn(
                "Health_Advisory",
                when(col("AQI_Category").equalTo("Good"),
                        "Enjoy outdoor activities")

                .when(col("AQI_Category").equalTo("Moderate"),
                        "Sensitive groups should be careful")

                .when(col("AQI_Category").equalTo("Unhealthy"),
                        "Wear masks outdoors")

                .when(col("AQI_Category").equalTo("Very Unhealthy"),
                        "Avoid outdoor activities")

                .otherwise(
                        "Stay indoors. Health emergency")
        );

        System.out.println("\nHealth Advisory:");

        advisory.show(false);
        
        System.out.println("Final Column //");
        
        Dataset<Row> finalOutput = advisory.select(
                zoneProfile.col("zone_id"),
                col("zone_name"),
                col("population"),
                col("num_schools"),
                col("aqi"),
                col("AQI_Category"),
                col("Health_Advisory"),
                col("event_time")
        );

        System.out.println("\nFinal Output:");

        finalOutput.show(false);
        
//        finalOutput.coalesce(1)
//        .write()
//        .mode("overwrite")
//        .option("header", "true")
//        .csv("output/health_advisory");
        
        
        System.out.println("\nHealth Advisory Report Saved Successfully!");
        
        
        spark.stop();
    }
}