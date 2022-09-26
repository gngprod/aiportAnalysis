package com.example
package jobs

import schemas.Schema
import readers.{CsvReader, InPathCsv}
import transformers.{AirportTop10AirlinesAndDestinationAirport, CountFlightReasonDelay,
  ProportionOfDelay, Top10AirlineNoDelay, Top10Airport, TopDayOfWeekNoDelay}

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
//    topAirlinesNoDelay.show()

    val airportTop10AirlinesAndOutAirport = flightsDF.transform(AirportTop10AirlinesAndDestinationAirport.begin)
//    airportTop10AirlinesAndOutAirport.show()

    val topDayOfWeekNoDelay = flightsDF.transform(TopDayOfWeekNoDelay.begin)
//    topDayOfWeekNoDelay.show()

    val countFlightReasonDelay = CountFlightReasonDelay.begin(flightsDF)
//    println(s"count Flight Reason Delay = $countFlightReasonDelay")

    val proportionOfDelay = flightsDF.transform(ProportionOfDelay.begin)
    proportionOfDelay.show()
  }
}
