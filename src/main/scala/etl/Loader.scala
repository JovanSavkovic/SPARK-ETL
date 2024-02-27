import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Loader {
  def loadData(outputDF: DataFrame, outputPath: String): Unit = {
    outputDF.write.mode(SaveMode.Overwrite).csv(outputPath)
  }
}
