
// spark-submit --class "Northwind" --master local[4] target\scala-2.10\spark_2.10-1.0.jar
object Northwind {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql._


  case class Employee(employeeId: Int, lastName: String, firstName: String,
                      title: String, birthDate: String, hireDate: String,
                      city: String, state: String, zip: String,
                      country: String, ReportsTo: String)

  case class Order(orderID: Int, customerID: String, employeeID: Int, orderDate: String, shipCountry: String)

  case class OrderDetail(orderID: Int, productId: Int, unitPrice: Double, qty: Double, discount: Double)

  def toId(in: String): Int = in.trim.toInt

  def toDouble(in: String): Double = in.trim.toDouble

  //http://maciejb.me/2014/07/11/a-csv-parser-moving-from-scala-parser-combinators-to-parboiled2/
  def csv2Employee(in: String): Employee =
    in.split(",") match {
      case Array(employeeId, lastName, firstName, title, birthDate, hireDate, city, state, zip, country, reportsTo, _*) => {
        Employee(employeeId.trim.toInt, lastName, firstName, title, birthDate, hireDate, city, state, zip, country, reportsTo)
      }
    }


  def csv2Order(in: String): Order =
    in.split(",") match {
      case Array(orderID, customerID, employeeID, orderDate, shipCountry, _*) => Order(toId(orderID), customerID, toId(employeeID), orderDate, shipCountry)
    }


  def csv2OrderDetails(in: String): OrderDetail =
    in.split(",") match {
      case Array(orderID, productId, unitPrice, qty, discount, _*) => OrderDetail(toId(orderID), toId(productId), toDouble(unitPrice), toDouble(qty), toDouble(discount))
    }


  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Northwind")
    val sql = new SQLContext(sc)
    import sql.implicits._

    //Employees
    val employeeFile = sc.textFile("C:/scala/spark/data/NW-Employees-NoHdr.csv")
    val employees = employeeFile.map(csv2Employee).toDF()
    employees.registerTempTable("employees")

    val r1 = sql.sql("SELECT * FROM employees")
    r1.foreach(println)

    val r2 = sql.sql("SELECT * FROM employees WHERE state = 'WA'")
    r2.foreach(println)

    //Orders
    val orderFile = sc.textFile("C:/scala/spark/data/NW-Orders-NoHdr.csv")
    val orders = orderFile.map(csv2Order).toDF()
    orders.registerTempTable("orders")

    val orderDetailFile = sc.textFile("C:/scala/spark/data/NW-Order-Details-NoHdr.csv")
    val orderDetails = orderDetailFile.map(csv2OrderDetails).toDF()
    orderDetails.registerTempTable("orderDetails")

    val r3 = sql.sql("SELECT orders.orderID, COUNT(*) as v FROM orders JOIN orderDetails ON orders.orderID = orderDetails.orderID GROUP BY orders.orderID ORDER BY v DESC")
    r3.take(10).foreach(println)
  }
}
