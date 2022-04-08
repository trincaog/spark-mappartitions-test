import org.mongodb.scala._
import org.mongodb
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class Product(
    productId: String,
    category: String,
    name: String,
    image: Array[Byte]
)

case class Sale(
    id: String,
    sale_id: String,
    customer_id: String,
    item: String,
    price: Double
)

case class SaleInfo(
    productId: String,
    category: String,
    amount: Int,
    product: Option[Product]
) {
  def this(sale: SaleInfo, product: Option[Product]) {
    this(sale.productId, sale.category, sale.amount, product)
  }

}
