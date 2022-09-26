package com.example
package readers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object CsvReader extends App with SessionWrapper {
  def read(filePath: String, schema: StructType, separator: Char = ',', hasHeader: Boolean = true): DataFrame =
    spark.read
      .schema(schema)
      .option("header", hasHeader.toString.toLowerCase)
      .option("sep", separator.toString)
      .csv(filePath)
}
