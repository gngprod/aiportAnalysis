package com.example
package jobs

import schemas.Schema
import readers.CsvReader
import transformers.{AirportTop10AirlinesAndDestinationAirport, CountFlightReasonDelay, ProportionOfDelay, Top10AirlineNoDelay, Top10Airport, TopDayOfWeekNoDelay}
import metadata.WriteMetaInfo
import paths.PathsCsv
import org.apache.spark.sql.DataFrame

object Job extends PathsCsv with Schema {

  def run(): Unit = {
    lazy val airlinesDF: DataFrame = CsvReader.read(airlinesPath, AirportSchema)
    lazy val airportsDF: DataFrame = CsvReader.read(airportsPath, AirlineSchema)
    lazy val flightsDF: DataFrame = CsvReader.read(flightsPath, FlightSchema)

    airlinesDF.show()
    airportsDF.show()
    flightsDF.show()

    val topAirports = flightsDF.transform(Top10Airport.begin)
    topAirports.show()

    val topAirlinesNoDelay = flightsDF.transform(Top10AirlineNoDelay.begin)
    topAirlinesNoDelay.show()

    val airportTop10AirlinesAndOutAirport = flightsDF.transform(AirportTop10AirlinesAndDestinationAirport.begin)
    airportTop10AirlinesAndOutAirport.show()

    val topDayOfWeekNoDelay = flightsDF.transform(TopDayOfWeekNoDelay.begin)
    topDayOfWeekNoDelay.show()

    val countFlightReasonDelay = CountFlightReasonDelay.begin(flightsDF)
    println(s"count Flight Reason Delay = $countFlightReasonDelay sec")

    val proportionOfDelay = flightsDF.transform(ProportionOfDelay.begin)
    proportionOfDelay.show()

    WriteMetaInfo.write(flightsDF)
  }
}
