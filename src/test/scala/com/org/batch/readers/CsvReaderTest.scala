package com.org.batch.readers

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, GlobalConfigBuilder}
import com.org.batch.factory.ReaderFactory
import com.org.batch.schemas.{SchemaManager, SchemaManagerParser, SchemaManagerReader}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CsvReaderTest extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    this.sparkSession = SparkSession.builder().appName("test-spark-session").master("local[*]").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    this.sparkSession.stop()
  }

  test("Test csv reader generates dataframe") {
    val params = new GlobalConfigBuilder()
      .withReaderType(Some("csv"))
      .withInputSource(Some(s"file://${getClass.getResource("/data/movies_metadata.csv").getPath}"))
      .build()

    val df = new ReaderFactory[GlobalConfig](this.sparkSession, params,
      new Parser("movies_metadata"),
      Map(
        SchemaManagerParser -> new SchemaManager("movies_metadata", SchemaManagerParser),
        SchemaManagerReader -> new SchemaManager("movies_metadata", SchemaManagerReader)
      )
    ).getInstance(ReaderFactory.CsvReader).read()

    assert(df != None)
    assert(df.get.count != 0)
    df.get.show(10, false)
  }
}
