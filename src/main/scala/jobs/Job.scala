package com.mainDir
package jobs

import schemas.Schema
import readers.CsvReader
import metrics.top10Airport

object Job extends App with csvPath with Schema{
  lazy val airlinesDF = CsvReader.read(airlinesPath, Airport)
  lazy val airportsDF = CsvReader.read(airportsPath, Airline)
  lazy val flightsDF = CsvReader.read(flightsPath, Flight)

  def run(): Unit = {
    airlinesDF.show()
    airportsDF.show()
    flightsDF.show()

    val topAirports = flightsDF.transform(top10Airport.begin)
    topAirports.show()
  }
}
