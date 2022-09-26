package com.example

import jobs.Job
import org.apache.log4j.{Level, Logger}

object FlightAnalyzer extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  Job.run()
}