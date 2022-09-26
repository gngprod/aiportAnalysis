package com.example
package writers

import com.example.SessionWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}

object CsvWriters extends App with SessionWrapper with OutPathCsv {
  def write(df: DataFrame, nameFile: String, separator: Char = ',', hasHeader: Boolean = true): Unit =
    df.write
    .mode(SaveMode.Overwrite)
    .save(OutPath + s"/$nameFile.csv")
}
