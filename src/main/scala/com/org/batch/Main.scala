package com.org.batch

import com.org.batch.config.{CLIParams, GlobalConfig, JobConfig}
import com.org.batch.factory.JobFactory

object Main extends App {
  def main(args: Seq[String]): Unit = {
    args.headOption match {
      case None => throw new Exception("Expected config manager to be present, but None found")
      case Some(_) => {
        val cliParams = new CLIParams().buildCLIParams(args)
        new JobFactory(cliParams).getInstance(cliParams.appName.getOrElse(JobFactory.Csv2Mongo.toString))
          .run()
      }
    }
  }
}
