import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.Date
import org.apache.spark.sql.streaming.Trigger

object MainStreamMapPartitions {
  def productLookup(spark: SparkSession, streamingDS: Dataset[SaleInfo]): Dataset[SaleInfo] = {
    import spark.implicits._

    streamingDS.mapPartitions(partition => {
      // setup DB connection
      val dbService = new ProductService()
      dbService.connect()

      partition.map(sale => {
        // Product lookup and merge
        val product = dbService.findProduct(sale.productId)
        new SaleInfo(sale, Some(product))
      }).iterator
    })
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._

    // val salesDS = Seq(
    //   SaleInfo("1", "c1", 2, None),
    // ).toDS

    val salesDS = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .map(x=>SaleInfo("1", "c1", 2, None)).as[SaleInfo]

    val df1 = productLookup(spark, salesDS)

    df1.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    Thread.sleep(3600 * 1000)
  }
}
