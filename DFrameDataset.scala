/** DataFrameDatasetTutorial2020 IntelliJ
 * Working thru the Spark Guide -- basic structured operations
 * 1. Read in the Flight data as in Ch2. did some transforms on it, "sort", column renamed
 * 2. created DataFrame from scratch, using createDatFrame(rdd,schema) and from raw scala
 * 3. case class to create a Dataset that has embedded nulls
 * 2020-08-25 rr
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
/* ** a couple of raw lines from the flight "summary-2015.csv"  file
DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count
United States,Romania,15
United States,Croatia,1
United States,Ireland,344
 */
object DFrameDataset {
def main(args: Array[String]):Unit = {
Logger.getLogger("org").setLevel(Level.OFF)
println(s"DataFrameDatasetTutorial ${new java.util.Date()} ")// seamless integration with Java
type S= String; type I = Integer; type D = Double; type B = Boolean// type synonyms
val spark = SparkSession
  .builder()
  .appName("DataFrameDatasetTutorial2020")
  .master("local[*]")
  .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
//println(s" Spark version: ${spark.version}")
val flightSchema = StructType( Array(
            StructField("DEST_COUNTRY_NAME", StringType, false),
            StructField("ORIGIN_COUNTRY_NAME", StringType, false),
            StructField("count", LongType, false)
))
val filePath = "/Users/rob/Desktop/2015-summary.csv"
// Spark creates a DataFrame, using the column names and types from schema
val df = spark.read
  .option("header","true")
  .format("csv")
  .schema (flightSchema)
  .load(filePath)
//df.printSchema()
//df.show(5, false)
val dfSorted = df.sort(desc("count"))
dfSorted.show(5, false)
val arrayOfRows = dfSorted.take(5)
arrayOfRows.foreach(println)
// let me rename columns for less clutter
val dfRenamed = df.withColumnRenamed("DEST_COUNTRY_NAME","destination")
                   .withColumnRenamed("ORIGIN_COUNTRY_NAME","origin")
dfRenamed.show(5, false)
dfRenamed.columns
dfRenamed.createOrReplaceTempView("dfRenamedTable")
// New 0829***continuing from chap 2
val sqlWay = spark.sql("""
 |Select destination, sum(count) as destination_total
 |From dfRenamedTable
 |Group By destination
 |Order By sum(count)
 |Limit 5
 |""".stripMargin)







println("before range4, printSchema()")
val range4 = spark.range(4)
range4.printSchema()
println("before range4 collected")
val collected = range4.collect()

println(s" collected $collected" )
val dfRange = range4.toDF("nr")

print(" dfRange show and dfRange printSchema ")
dfRange.show()
dfRange.printSchema()


/*
val dfn = spark.range(5).toDF("nr")
dfn.show()

println (s" dfn.dtypes  ${dfn.dtypes}" )

val dsn = dfn.as[Nr]
def testMethod( i: Int): Int= i * 10
//dsn.map( row => testMethod(row.nr))
*/
// Creating a DF from scratch: below is a test schema, roughly as on pg 67 the Spark book ( I renamed columns)
val rddSchema = StructType(Array(
StructField("name",StringType, true),
StructField("status",StringType, true),
StructField("qty",LongType, false)
))
//
val r1 = Row( "A","OK", 42L)
val r2 = Row( "B",null, 21L)
val myRows = Seq( r1,  r2  )
val rdd = spark.sparkContext.makeRDD(myRows)   // parallelize --- synonym makeRDD
val dfRDD = spark.createDataFrame(rdd,rddSchema)
//dfRDD.show(5)
val scalaShortCut = Seq(( "A","OK", 42L),( "B",null, 21L)).toDF("name", "status", "qty")
scalaShortCut.show(2)
val ds = scalaShortCut.as[caseRDD]
println(s" DS: case class enforced on scalaShortCut DF")
ds.show()
ds.dtypes.foreach(println)
 }//end main
}//end object
case class Nr(nr: BigInt)
case class caseRDD( name: String, status: String, qty : Long)
