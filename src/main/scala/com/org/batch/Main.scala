package com.org.batch

import com.org.batch.config.{CLIParams, GlobalConfig, JobConfig}
import com.org.batch.factory.JobFactory

object Main extends App {
  def main(args: Seq[String]): Unit = {
    args.headOption match {
      case None => throw new Exception("Expected config manager to be present, but None found")
      case Some(cm) => {
        new JobFactory(args).getInstance(cm)
          .run()
      }
    }
  }
}
