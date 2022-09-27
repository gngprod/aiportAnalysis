package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object ProportionOfDelay {

  def calculationSumDelay(df: DataFrame): Any = {
    val sumDelay = df
      .select((
        sum("AIR_SYSTEM_DELAY") +
        sum("SECURITY_DELAY") +
        sum("AIRLINE_DELAY") +
        sum("LATE_AIRCRAFT_DELAY") +
        sum("WEATHER_DELAY")).as("SUM_DELAY"))

    sumDelay.collect()(0)(0)
  }

  def calculationProportion(x: Column, sumDelay: Any): Column = {
    round(sum(x) * 100 / sumDelay, 2).as(s"PROPORTION_$x")
  }

  def begin(df: DataFrame): DataFrame = {
    val sumDelay: Any = calculationSumDelay(df)
//    println(sumDelay)

    val proportionOfDelayDF = df
      .select(
        calculationProportion(col("AIR_SYSTEM_DELAY"), sumDelay),
        calculationProportion(col("SECURITY_DELAY"), sumDelay),
        calculationProportion(col("AIRLINE_DELAY"), sumDelay),
        calculationProportion(col("LATE_AIRCRAFT_DELAY"), sumDelay),
        calculationProportion(col("WEATHER_DELAY"), sumDelay))

    proportionOfDelayDF
  }
}
