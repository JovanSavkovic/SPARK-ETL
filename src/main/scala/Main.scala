import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("ETLJob")

    // API key is generated per user, so cannot be hardcoded
    // Generate your own key from the alphavantage website and then in the shell
    // you are running the code from
    // export ALPHAVANTAGE_API_KEY="your_key"
    val apiKey = sys.env.get("ALPHAVANTAGE_API_KEY") match {
      case Some(key) => key
      case None => println("Environment variabl ALPHAVANTAGE_API_KEY does not exist")
    }

    // Extract data for prices of wheat
    val wheatUrl = s"https://www.alphavantage.co/query?function=WHEAT&interval=monthly&apikey=$apiKey"
    val inputWheatDF = Extractor.extractData(spark, wheatUrl)

    // Extract data for prices of corn
    val cornUrl = s"https://www.alphavantage.co/query?function=CORN&interval=monthly&apikey=$apiKey"
    val inputCornDF = Extractor.extractData(spark, cornUrl)

    val transformedWheatDf = Transformer.transformData(inputWheatDF, "WHEAT")

    transformedWheatDf.show
    spark.stop()
  }
}
