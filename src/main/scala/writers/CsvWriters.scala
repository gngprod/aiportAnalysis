package com.example
package writers

import paths.PathsCsv
import org.apache.spark.sql.{DataFrame, SaveMode}

object CsvWriters extends PathsCsv {

  def write(df: DataFrame, nameFile: String): Unit =
    df.write
    .mode(SaveMode.Overwrite)
    .save(OutPath + nameFile + ".csv")
}
