/**DataFrameDatasetTutorial2020. RetailData
 * Ch 6 Spark guide "Working with different types of Data
 * biils, Numbers, Strings, Dates timestamps Null, Complex types
 * UDfs
 * 1. read in retail file ( note, if you dont spec the format
 * as CSV, it defaults to parquet and so you'll get an error !!
 *
 * 2020-09-23 rr
 */
//import breeze.linalg.norm
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, map, expr, pow,  explode}
import org.apache.spark.sql.Row

import scala.collection.immutable.Vector
import plotly._
import element._
import layout._
import Plotly._



object RetailData extends App{
Logger.getLogger("org").setLevel(Level.OFF)
type I = Int ; type S = String; type D = Double; type B = Boolean
import Utilities._

val spark = SparkSession.builder().appName("DatasetCreateManiplate")
.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
// this is pg 84 retail data by day
val path = "/Users/rob/Desktop/retail-2010-12-01.csv"
val mySchema = StructType(Array(
 StructField("InvoiceNo", StringType, true),
 StructField("StockCode", StringType, true),
StructField("Description", StringType, true),
StructField("Quantity", DoubleType, true),
StructField("InvoiceDate", TimestampType, true),
StructField("UnitPrice", DoubleType, true),
StructField("CustomerID", DoubleType, true),
StructField("Country", StringType, true)
))
case class InvoiceData(InvoiceNo:String, StockCode:String, Description:String,
                       Quantity:Double, InvoiceDate: java.sql.Timestamp,
                       UnitPrice:Double, CustomerID:Double,Country:String )

val df1 = spark.read
   .format("csv")
   .option("header","true")
   .schema(mySchema)
   .load(path)
df1.show(3,false)
println(s" print schema of DataFrame df1")
df1.printSchema()
println(s" now change to a Dataset using  case class")
val ds1 = df1.as[InvoiceData]
println(s" print schema of Dataset ds1")

ds1.printSchema()
ds1.createOrReplaceTempView("ds1Table")
 spark.sql("Select 5, 'five', 5.0  ").show(3)
ds1.select(lit(1),lit("five"), lit(5.23) ).show(3)
ds1.filter('InvoiceNo .equalTo (536365))
.select('InvoiceNo, 'Description)
.show(3,false)
println(
s"""Set up filters for ds1, filtering on for {'DOT, price, description}
  |NOTICE I use Scala shorthand, a single quote selects the column
  |although the column's name is in double quotes""".stripMargin)
val DOTCodeFilter = 'StockCode === "DOT"
val priceFilter  = 'UnitPrice > 600
val descriptionFilter = 'Description.contains( "POSTAGE")
println(
s"""Create a new col,  "isExpensive" ( col name  must be a String, column reference
  |may use a single quote mark or col("columnName")
  | then (filter) out  only rows   where 'isExpensive  equates to a true
  | """.stripMargin)
val expensiveStuff= ds1.withColumn("isExpensive" ,
      DOTCodeFilter.and (priceFilter .or (descriptionFilter)))
      .filter('isExpensive)
.select("unitPrice", "isExpensive")
expensiveStuff.show(5)
// be careful, this isn't the scala math pow!!! it is the Spark sql pow!!
val fabricatedQuantity =
         pow( col("Quantity") * col("UnitPrice"), 2 ) + 5

val adjustedQty = ds1.select('CustomerId, round(fabricatedQuantity,2).alias("AdjustedQuantity"))
adjustedQty.show(3, false)

println(s"calc correlation value Quantity vs UnitPrice  = -0.041 ???")

val correlate = ds1.select(corr('Quantity, 'UnitPrice))
correlate.show()
println(s"select and collect() quantities and unitprices to do an ad-hoc correlation check")
println(s""" print out five quantities and prices, quantities are already Scala Doubles
             as are UnitPrices""".stripMargin )

println(s" collect all the quantity values , as Row of Doubles, test out 5 of each them")
val qtys = ds1.select('Quantity).collect()
val qtys5= qtys.take(5)
println(s" testing just 5 elements qtys $cvtqtys5 , Next I'll convert all 3100 !")

val cvtqtys5 = qtys5.map( row => row.getDouble(0)).toVector

println(s" cvtqtys5 $cvtqtys5")
val prices = ds1.select('UnitPrice).collect()
val prices5 = prices.take(5)
val cvtprices5 = prices5.map(_ .getDouble(0)).toVector
println(s" cvtprices5 $cvtprices5")
val testCorrelation = Utilities.correlate(cvtprices5,cvtqtys5)
println(s" test of correlation cvtprices5 cvtqtys $testCorrelation")
//  Now convert all the quantities and unnitprices to Vector[Double]
println(s" converting all 3100 prices and quantities to a Scala Vector ")
val cvtprices = prices.map(row => row.getDouble(0)).toVector
val cvtqtys = qtys.map(_ .getDouble(0)).toVector
println(s" qtys.length, prices.length   ${qtys.length} , ${prices.length}")

println(s" Now I will do correlation between Quantity and UnitPrice @ 3100 elements apiece! ")

val corrQtyPrice = dot(cvtprices,cvtqtys)/(norm(cvtprices)* norm(cvtqtys))
println(s" correlation coefficient is: ${corrQtyPrice} ")

ds1.describe().show()
val complexDF = ds1.selectExpr("( Description,InvoiceNo) as complex", "*")
complexDF.show(5,false)

val arrayTest = ds1.select( split ('Description, " "))
println(s" splitting the Description component of ds1 into tokens")
arrayTest.show()

println(s" now try a map operations on Description -> InvoiceNo")
//val mapper = ds1.select(map( 'Description, 'InvoiceNo).alias("complex map")



// plotting stuff in plotly
println(s" a little plot work here 'plotly' ")
val plots = Seq(
Scatter( cvtprices,cvtqtys,  name = "unitprice vs qty")
)
plots.plot(title = "price vs qty")



}//end object

