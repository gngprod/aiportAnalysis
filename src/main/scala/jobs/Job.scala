package com.mainDir
package jobs

import schemas.Schema
import readers.{CsvReader, InPathCsv}
import transformers.{Top10AirlineNoDelay, Top10Airport}

object Job extends App with InPathCsv with Schema {
  lazy val airlinesDF = CsvReader.read(airlinesPath, Airport)
  lazy val airportsDF = CsvReader.read(airportsPath, Airline)
  lazy val flightsDF = CsvReader.read(flightsPath, Flight)

  def run(): Unit = {
    airlinesDF.show()
    airportsDF.show()
    flightsDF.show()

    val topAirports = flightsDF.transform(Top10Airport.begin)
//    topAirports.show()

    val topAirlinesNoDelay = flightsDF.transform(Top10AirlineNoDelay.begin)
    topAirlinesNoDelay.show()
  }
}
