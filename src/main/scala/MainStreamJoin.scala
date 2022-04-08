import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.Date
import org.apache.spark.sql.streaming.Trigger

object MainStreamJoin {
  def joinWithProduct(spark: SparkSession, streamingDS: Dataset[SaleInfo]): Dataset[SaleInfo] = {
    import spark.implicits._
    
    val staticDS = spark.read
      .format("parquet")
      .load("/tmp/prods.parquet").as[Product]

    streamingDS
      .joinWith(staticDS, 
        streamingDS("productId")===staticDS("productId") && 
        streamingDS("category")===staticDS("category"))   
      .map{ 
        case (sale,product) => new SaleInfo(sale, Some(product))
      }
    
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

    val df1 = joinWithProduct(spark, salesDS)

    df1.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    Thread.sleep(3600 * 1000)
  }
}
