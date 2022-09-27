package com.example
package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CountFlightReasonDelay {

  def begin(df: DataFrame): Long = {
    val countFlightReasonDelayDF = df
      .select("FLIGHT_NUMBER", "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
      .filter(
        col("AIR_SYSTEM_DELAY") > 0 or
          col("SECURITY_DELAY") > 0 or
          col("AIRLINE_DELAY") > 0 or
          col("LATE_AIRCRAFT_DELAY") > 0 or
          col("WEATHER_DELAY") > 0)
      .count()

    countFlightReasonDelayDF
  }
}
