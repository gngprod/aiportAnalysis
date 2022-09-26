package com.example
package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Top10AirlineNoDelay extends App {
  def begin(df: DataFrame): DataFrame = {
    val topAirportNoDelay: DataFrame = df.filter(col("DEPARTURE_DELAY") <= 0)
      .groupBy("AIRLINE").count()
      .orderBy(col("count").desc)
      .limit(10)
      .withColumn("top", row_number.over(Window.orderBy(col("count").desc)))
      .select("top", "AIRLINE", "count")
    topAirportNoDelay
  }
}
