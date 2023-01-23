package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, mean, row_number}
import org.apache.spark.sql.expressions.Window
import spire.implicits.eqOps

import java.util.Properties

object FullLoad {
  def main(args: Array[String]): Unit = {

    // create sparksession
    val spark = SparkSession
      .builder()
      .appName("full_load")
      .enableHiveSupport()
      .master("local[16]")
      .getOrCreate()

    // load normal customer and sales details from postgresql
    val url: String = "jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "consultants")
    connectionProperties.put("password", "WelcomeItc@2022")
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    val customer_table_name: String = "customers_scala_joseph"
    val sales_table_name: String = "sales_details_scala_joseph"
    val customers = spark.read.jdbc(url, customer_table_name, connectionProperties)
    val sales_details = spark.read.jdbc(url, sales_table_name, connectionProperties)

    // load normal datasets to HIVE
    customers.write.saveAsTable("scala_data.customers_joseph")
    sales_details.write.saveAsTable("scala_data.sales_details_joseph")

    // transformations


    // 1.) change datatype of UnitPrice, OrderQty and LineTotal from String to Double
    val sales_details_2 = sales_details.select(col("customerId"),
      col("ProductID").cast("String"),
      col("OrderQty").cast("Double"),
      col("UnitPrice").cast("Double"),
      col("LineTotal").cast("Double")
    )

    // 2.) count nulls to see if they exist in UnitPrice
    val nullCount = sales_details_2.select(col("UnitPrice")).filter(col("UnitPrice").isNull).count()

    // create sales_details_4 variable in-case if statement is not used
    val sales_details_4 = sales_details_2

    // if nanCount is larger than zero then perform averaging on null values
    if (nullCount > 0) {

      // 3.) get mean of UnitPrice
      val unit_price_ave = sales_details_2.select(Seq("UnitPrice").map(mean): _*).first.getDouble(0)

      // 4.) replace null values in unit price with ave
      val sales_details_3 = sales_details_2.na.fill(unit_price_ave: scala.Double, Seq("UnitPrice"))

      // 5.) recalculate LineTotal data with new non null UnitPrice column
      val sales_details_4 = sales_details_3.withColumn("LineTotal",
        col("UnitPrice") * col("OrderQty"))
    }
    // 6.) Groupby and get counts of products purchased for each customer
    val customer_products = sales_details_4.groupBy("customerId", "ProductID").count()

    // 7.) Get the top purchased product for each customer
    val windowed_df = Window.partitionBy("customerId").orderBy(col("count").desc)
    val customer_fav_prods = customer_products.withColumn("row", row_number.over(windowed_df)).filter(col("row") === 1).drop("row")
    val customer_fav_prods_2 = customer_fav_prods.select(col("customerId"), col("ProductID")).withColumnRenamed("ProductID", "FaveProductID")

    // 8.) Groupby and get total spent by each customer
    val customer_spend = sales_details_4.groupBy("customerId").sum().drop("sum(UnitPrice)")
    val customer_spend_2 = customer_spend.withColumnRenamed("sum(OrderQty)", "TotalOrders").withColumnRenamed("sum(LineTotal)", "TotalSpent")

    // 9.) Merge customer_fav_prods_2 and customer_spend on CustomerId and rename
    val merged_data_1 = customer_spend_2.join(customer_fav_prods_2, "customerId")

    // 10.) Rename CustomerKey value in customers to CustomerId for join and drop some columns
    val customers_2 = customers.withColumnRenamed("CustomerKey", "customerId")
    val customers_3 = customers_2.drop("MaritalStatus", "NumberCarsOwned", "NumberChildrenAtHome",
      "TotalChildren", "Education", "Houseowner", "CommuteDistance",
      "Country", "DaySinceFirstOrder")

    // 11.) Merge with customers_2 dataset
    val merged_data_2 = customers_3.join(merged_data_1, "customerId")

    println(merged_data_2.show(10))

    // load transformed dataset to HIVE
    merged_data_2.write.saveAsTable("scala_data.transformed_data_joseph")
  }
}
