package com.example
package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TopDayOfWeekNoDelay {

  def begin(df: DataFrame): DataFrame = {
    val topDayOfWeekNoDelayDF = df
      .select("DAY_OF_WEEK", "ARRIVAL_DELAY")
      .groupBy("DAY_OF_WEEK").avg("ARRIVAL_DELAY")
      .select(col("DAY_OF_WEEK"), round(col("avg(ARRIVAL_DELAY)"), 2).as("AVG_ARRIVAL_DELAY"))
      .orderBy("AVG_ARRIVAL_DELAY")

    topDayOfWeekNoDelayDF
  }
}
