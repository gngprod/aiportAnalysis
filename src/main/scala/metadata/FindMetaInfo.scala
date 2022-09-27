package com.example
package metadata

import schemas.Schema

import paths.PathsCsv
import org.apache.spark.sql.{AnalysisException, DataFrame}

object FindMetaInfo extends App with Schema with PathsCsv with SessionWrapper {

  try {
    val checkMetaDataDF: DataFrame = spark.read
      .schema(MetaDataSchema)
      .csv(MetaInfoPath)
    checkMetaDataDF.show()
  } catch {
    case e: AnalysisException => println("Couldn't find that file.")
  }
}
