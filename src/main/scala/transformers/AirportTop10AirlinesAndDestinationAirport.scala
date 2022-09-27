package com.example
package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AirportTop10AirlinesAndDestinationAirport {

  def topAirline(df: DataFrame): DataFrame = {
    val topAirlineDF: DataFrame = df.filter(col("DEPARTURE_DELAY") <= 0)
      .groupBy("ORIGIN_AIRPORT", "AIRLINE").count()
      .withColumn("TOP_AIRLINE",
        row_number.over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(col("count").desc)))
      .filter(col("TOP_AIRLINE") <= 10)
//      .filter(col("ORIGIN_AIRPORT") === "LAX")
      .select("TOP_AIRLINE","ORIGIN_AIRPORT", "AIRLINE")
    topAirlineDF
  }

  def topDestinationAirport(df: DataFrame): DataFrame = {
    val topDestinationAirportDF = df.filter(col("DEPARTURE_DELAY") <= 0)
      .groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT").count()
      .withColumn("TOP_DESTINATION_AIRPORT",
        row_number.over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(col("count").desc)))
      .filter(col("TOP_DESTINATION_AIRPORT") <= 10)
//      .filter(col("ORIGIN_AIRPORT") === "LAX")
      .select("TOP_DESTINATION_AIRPORT","ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
    topDestinationAirportDF
  }

  def begin(df: DataFrame): DataFrame = {
    val topAirlineDF = topAirline(df)

    val topDestinationAirportDF = topDestinationAirport(df)

    val joinOn = topAirlineDF.col("ORIGIN_AIRPORT") === topDestinationAirportDF.col("ORIGIN_AIRPORT") and
      topAirlineDF.col("TOP_AIRLINE") === topDestinationAirportDF.col("TOP_DESTINATION_AIRPORT")

    val topAirlineAndDestinationAirportDF = topAirlineDF.join(topDestinationAirportDF, joinOn, "Inner")
      .withColumnRenamed("TOP_AIRLINE", "TOP_AIRLINE_AND_DESTINATION_AIRPORT")
      .select(
        col("TOP_AIRLINE_AND_DESTINATION_AIRPORT"),
        topAirlineDF.col("ORIGIN_AIRPORT"),
        col("AIRLINE"),
        col("DESTINATION_AIRPORT"))
//      .filter(col("ORIGIN_AIRPORT") === "LAX")

    topAirlineAndDestinationAirportDF
  }
}
