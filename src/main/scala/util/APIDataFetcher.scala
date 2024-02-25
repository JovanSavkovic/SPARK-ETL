import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object APIDataFetcher {
  def fetchData(apiUrl: String): String = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(apiUrl)
    val response = httpClient.execute(httpGet)
    val jsonData = EntityUtils.toString(response.getEntity())
    jsonData.toString
  }
}
