package com.mainDir
package readers

import org.apache.spark.sql.SparkSession

trait SessionWrapper {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .master("local")
    .getOrCreate()
}
