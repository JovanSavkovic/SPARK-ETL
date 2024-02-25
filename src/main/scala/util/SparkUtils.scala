import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.master", "local[*]")
      .getOrCreate()
  }
}
