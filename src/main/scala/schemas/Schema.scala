package com.example
package schemas

import org.apache.spark.sql.types._

trait Schema {
  val Airport: StructType =
    StructType(Seq(
    StructField("IATA_CODE", StringType),
    StructField("AIRLINE", StringType)
    ))

  val Airline: StructType =
    StructType(Seq(
      StructField("IATA_CODE", StringType),
      StructField("AIRPORT", StringType),
      StructField("CITY", StringType),
      StructField("STATE", StringType),
      StructField("COUNTRY", StringType),
      StructField("LATITUDE", DoubleType),
      StructField("LONGITUDE", DoubleType)
    ))

  val Flight: StructType =
    StructType(Seq(
      StructField("YEAR", IntegerType),
      StructField("MONTH", IntegerType),
      StructField("DAY", StringType),
      StructField("DAY_OF_WEEK", IntegerType),
      StructField("AIRLINE", StringType),
      StructField("FLIGHT_NUMBER", IntegerType),
      StructField("TAIL_NUMBER", StringType),
      StructField("ORIGIN_AIRPORT", StringType),
      StructField("DESTINATION_AIRPORT", StringType),
      StructField("SCHEDULED_DEPARTURE", IntegerType),
      StructField("EPARTURE_TIME", IntegerType),
      StructField("DEPARTURE_DELAY", DoubleType),
      StructField("TAXI_OUT", IntegerType),
      StructField("WHEELS_OFF", IntegerType),
      StructField("SCHEDULED_TIME", IntegerType),
      StructField("ELAPSED_TIME", IntegerType),
      StructField("AIR_TIME", IntegerType),
      StructField("DISTANCE", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("SCHEDULED_ARRIVAL", IntegerType),
      StructField("ARRIVAL_TIME", IntegerType),
      StructField("ARRIVAL_DELAY", DoubleType),
      StructField("DIVERTED", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("CANCELLATION_REASON", StringType),
      StructField("AIR_SYSTEM_DELAY", StringType),
      StructField("SECURITY_DELAY", StringType),
      StructField("AIRLINE_DELAY", StringType),
      StructField("LATE_AIRCRAFT_DELAY", StringType),
      StructField("WEATHER_DELAY", StringType)
    ))
}

//trait Schema {
//  case class Airport(
//                      IATA_CODE: String,
//                      AIRLINE: String
//                    )
//
//  case class Airline(
//                      IATA_CODE: String,
//                      AIRPORT: String,
//                      CITY: String,
//                      STATE: String,
//                      COUNTRY: String,
//                      LATITUDE: Double,
//                      LONGITUDE: Double
//                    )
//
//  case class Flight(
//                     YEAR: Int,
//                     MONTH: Int,
//                     DAY: String,
//                     DAY_OF_WEEK: Int,
//                     AIRLINE: String,
//                     FLIGHT_NUMBER: Int,
//                     TAIL_NUMBER: String,
//                     ORIGIN_AIRPORT: String,
//                     DESTINATION_AIRPORT: String,
//                     SCHEDULED_DEPARTURE: Int,
//                     DEPARTURE_TIME: Int,
//                     DEPARTURE_DELAY: Double,
//                     TAXI_OUT: Int,
//                     WHEELS_OFF: Int,
//                     SCHEDULED_TIME: Int,
//                     ELAPSED_TIME: Int,
//                     AIR_TIME: Int,
//                     DISTANCE: Int,
//                     WHEELS_ON: Int,
//                     TAXI_IN: Int,
//                     SCHEDULED_ARRIVAL: Int,
//                     ARRIVAL_TIME: Int,
//                     ARRIVAL_DELAY: Double,
//                     DIVERTED: Int,
//                     CANCELLED: Int,
//                     CANCELLATION_REASON: String,
//                     AIR_SYSTEM_DELAY: String,
//                     SECURITY_DELAY: String,
//                     AIRLINE_DELAY: String,
//                     LATE_AIRCRAFT_DELAY: String,
//                     WEATHER_DELAY: String
//                   )
//}
