import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{month, year, max, min, count, avg, col, lit}

object Transformer {
  def transformData(inputDF: DataFrame, grainType: String): DataFrame = {

    val dfFormated = inputDF.withColumn("month", month(col("date")))
                            .withColumn("year", year(col("date")))
                            .withColumn("value", col("value").cast("double"))
                            .select(col("year"), col("month"), col("value")).na.drop()

    val dfMaxMonthPerYear = dfFormated.groupBy(col("year"))
                              .agg(max(col("value")).alias("value"))
                              .orderBy(col("year"))
 
    val dfMaxMonthPerYearJoined = dfMaxMonthPerYear.join(dfFormated, 
                                                        Seq("year", "value"),
                                                            "inner").orderBy(col("year"))
  
    val dfMinMonthPerYear = dfFormated.groupBy(col("year"))
                              .agg(min(col("value")).alias("value"))
                              .orderBy(col("year"))
   
    val dfMinMonthPerYearJoined = dfMinMonthPerYear.join(dfFormated, 
                                                        Seq("year", "value"),
                                                            "inner").orderBy(col("year"))

    val dfCountMaxMonth = dfMaxMonthPerYearJoined.groupBy(col("month")).count()
    val dfCountMinMonth = dfMinMonthPerYearJoined.groupBy(col("month")).count()

    val dfMostCommonMaxMonth = dfCountMaxMonth.join(dfCountMaxMonth.agg(max(col("count")).alias("count")), Seq("count"), "inner").select(col("month").alias("max_month"))
    val dfMostCommonMinMonth = dfCountMinMonth.join(dfCountMinMonth.agg(max(col("count")).alias("count")), Seq("count"), "inner").select(col("month").alias("min_month"))
    
    val dfAverageYearlyFluctuations = dfMaxMonthPerYear.join(dfMinMonthPerYear, Seq("year"), "inner")
                                  .select(col("year"), 
                                          (dfMaxMonthPerYear("value") - dfMinMonthPerYear("value")).alias("fluctuation")
                                  ).agg(avg(col("fluctuation")).alias("fluctuation"))

    val dfCombined = dfAverageYearlyFluctuations.crossJoin(dfMostCommonMaxMonth).crossJoin(dfMostCommonMinMonth)
    dfCombined

  }
}
