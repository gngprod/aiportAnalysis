package com.example
package metadata

import schemas.Schema

import paths.PathsCsv
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate

object WriteMetaInfo extends SessionWrapper with PathsCsv with Schema {

  def findDate(df: DataFrame, nameColumn: String, aggTypeDate: String): String = {
    if (aggTypeDate.toLowerCase == "min"){
      df.groupBy().min(nameColumn).collect()(0)(0).toString
    } else if(aggTypeDate.toLowerCase == "max"){
      df.groupBy().max(nameColumn).collect()(0)(0).toString
    } else {
      "Error agg date."
    }
  }

  def findMinMaxDate(df: DataFrame, typeDate: String): String = {
    val yearDF = df.filter(col("YEAR") === findDate(df, "YEAR", typeDate))
    val yearMonthDF = yearDF.filter(col("MONTH") === findDate(yearDF, "MONTH", typeDate))
    val yearMonthDayDF = yearMonthDF.filter(col("DAY") === findDate(yearMonthDF, "DAY", typeDate))
    val date = yearMonthDayDF
      .select(concat(col("YEAR"), lit("-"), col("MONTH"), lit("-"), col("DAY")))
      .distinct()
      .collect()(0)(0).toString
    date
  }
  def findCollectedValue(df: DataFrame): String = {
    val collectValue: String = findMinMaxDate(df, "min") + ";" +  findMinMaxDate(df, "max")

    println(collectValue)
    collectValue
  }

  def newMetaDataInfo(df: DataFrame): DataFrame = {
    import spark.implicits._

    val analysisDiapason: String = findCollectedValue(df)
    val analysisDate: String = LocalDate.now().toString

    val newMetaDataInfoDF: DataFrame = Seq((analysisDiapason, analysisDate)).toDF("collected", "processed")
    newMetaDataInfoDF
  }

  def write(df: DataFrame): Unit = {
    val metaDataInfoDF: DataFrame = newMetaDataInfo(df)
    metaDataInfoDF.show()

    metaDataInfoDF.write
      .mode(SaveMode.Append)
      .csv(MetaInfoPath)
  }
}
