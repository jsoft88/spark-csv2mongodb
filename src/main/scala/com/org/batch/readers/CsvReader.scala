package com.org.batch.readers

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.schemas.{SchemaManager, SchemaManagerParser, SchemaManagerReader, SchemaManagerType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class CsvReader[+T <: JobConfig](sparkSession: SparkSession, config: T, configFile: Parser, schemaManager: Map[SchemaManagerType, SchemaManager])
  extends {
    private var inputSource = ""
    private var readerOptions: Map[String, String] = Map.empty
    private var readerSchema: Option[StructType] = None
    private var parserSchemaManager: Option[SchemaManager] = None
} with BaseReader[T](sparkSession, config, configFile, schemaManager) {

  override protected def setupReader(): Unit = {
    this.config.asInstanceOf[GlobalConfig].inputSource match {
      case None => throw new Exception("Expected input source to be present, but None found")
      case Some(is) => this.inputSource = is
    }
    this.readerOptions = this.configFile.getSparkOptions()
    this.readerSchema = this.schemaManager.get(SchemaManagerReader) match {
      case Some(rs) => Some(rs.getSchemaAsStructType())
      case None => throw new Exception("Missing reader schema manager in provided schema manager")
    }

    this.parserSchemaManager = this.schemaManager.get(SchemaManagerParser) match {
      case Some(ps) => Some(ps)
      case None => throw new Exception("Missing parser schema manager in provided schema manager")
    }
  }

  override def read(): Option[DataFrame] = {
    val df = this.sparkSession.read
      .options(this.readerOptions)
      .schema(this.readerSchema.get)
      .csv(this.inputSource)
    this.configFile.getJsonFields() headOption match {
      case None => Some(df)
      case Some(_) => {
        val jsonFields = this.configFile.getJsonFields()
        val castNonJsonCols = df.columns
          .filterNot(jsonFields.contains _)
          .map(c => functions.col(c).cast(this.parserSchemaManager.get.getSchemaForField(c).fields.head.dataType))
        jsonFields
          .foldRight(df
            .select(castNonJsonCols ++ jsonFields.map(functions.col(_)): _*)
          )((c, d) => d.withColumn(c, functions.from_json(df(c), this.parserSchemaManager.get.getSchemaForField(c))))
        Some(df)
      }
    }
  }
}
