import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{explode, col}

object Extractor {
  def extractData(spark: SparkSession, apiUrl: String): DataFrame = {
    import spark.implicits._
    val df = spark.read.json(Seq(APIDataFetcher.fetchData(apiUrl)).toDS)
    val explodedDF = df.withColumn("data", explode(col("data")))

    explodedDF.select(
      col("data.date").alias("date"),
      col("data.value").alias("value")
    )
  }
}
