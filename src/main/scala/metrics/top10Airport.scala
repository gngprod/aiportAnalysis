package com.mainDir
package metrics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object top10Airport {
  def begin(df: DataFrame): DataFrame = {
    val topAirportDF =
      df.select(col("ORIGIN_AIRPORT").as("AIRPORT"))
        .union(df.select(col("DESTINATION_AIRPORT").as("AIRPORT")))
        .groupBy("AIRPORT").count()
        .orderBy(col("count").desc)
        .limit(10)
        .withColumn("top", row_number.over(Window.orderBy(col("count").desc)))
        .select("top", "AIRPORT", "count")
    topAirportDF
  }
}
