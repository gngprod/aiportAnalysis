package com.example

import org.apache.spark.sql.SparkSession

trait SessionWrapper {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .master("local")
    .getOrCreate()
}
