package com.mainDir

import jobs.Job
import org.apache.log4j.{Level, Logger}

object Main extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  Job.run()
}