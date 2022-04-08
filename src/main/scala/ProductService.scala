import org.mongodb.scala._
import org.mongodb
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._

import java.util.Date
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ProductService {

  var client = None: Option[MongoClient];
  var db = None: Option[MongoDatabase];
  var collection = None: Option[MongoCollection[Document]];

  private def buildProduct(d: Document): Product = {
    Product(
      d.get("product_id").get.asString().getValue,
      d.get("category").get.asString().getValue,
      d.get("name").get.asString().getValue,
      d.get("image").get.asBinary().getData
    )
  }

  def connect(): Unit = {
    this.connect("mongodb://localhost", "demo", "products")
  }
  def connect(uri: String, dbName: String, collectionName: String): Unit = {
    this.client = Some(mongodb.scala.MongoClient(uri))
    this.db = Some(client.get.getDatabase(dbName))
    this.collection = Some(db.get.getCollection(collectionName))
  }

  def disconnect(): Unit = {
    this.client.get.close()
  }

  def findProduct(productId: String): Product = {
    val records =
      this.collection.get.find(equal("product_id", productId)).toFuture()

    Await.result(records, Duration.Inf)
    val recList = records.value.get.get.map(r => buildProduct(r))
    recList.head
  }

}
