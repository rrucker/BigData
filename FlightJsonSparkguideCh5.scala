/** DataFrameDatasetTutorial2020> FlightJsonSparkguideCh5
 * CH 5 of the Spark guide book, reading in json files, to create DFs
 * special scala syntax, a single quote mark, allows a column to be specified
 * e.g.  'destination , also could be $"destination" , col("destination")
 * pretty standard standard, but check out the randomSplit pattern matched into an
 * array of DataFrames :-)
 * ***** NEXT IS FROM THE DATABRICKS DOCS DESCRIPTION OF Datasets
 * "The Datasets API provides the benefits of RDDs (strong typing, ability to use powerful lambda
 * functions) with the benefits of Spark SQL’s optimized execution engine.
 * You can define a Dataset JVM objects and then manipulate them using functional transformations
 * (map, flatMap, filter, and so on) similar to an RDD. The benefits is that, unlike RDDs,
 * these transformations are now applied on a structured and strongly typed distributed
 * collection that allows Spark to leverage Spark SQL’s execution engine for optimization."
 * 2020-09-20/21 rr  ( added databricks documentation 09-22 )
 *  ********** json input file ********** on json object per line **
 * {"ORIGIN_COUNTRY_NAME":"Romania","DEST_COUNTRY_NAME":"United States","count":15}
 * {"ORIGIN_COUNTRY_NAME":"Croatia","DEST_COUNTRY_NAME":"United States","count":1}
 * {"ORIGIN_COUNTRY_NAME":"Ireland","DEST_COUNTRY_NAME":"United States","count":344}
 */
import breeze.linalg.*
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.Row

object FlightJsonSparkguideCh5{
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
println(s" Spark version ${spark.version}")
val path3 = "/Users/rob/Desktop/2015-summary.json"
println(s"Creating a DF with  a file read")
val dfRaw= spark.read.format("json")
              .load(path3)
val dfNewCols= dfRaw.withColumnRenamed("DEST_COUNTRY_NAME", "destination")
                      .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin")
println(s" Flight data with columns renamed")
dfNewCols.show(2, false)

println(s" just as an example: Showing three columns selected using Scala syntax")
val dfScalaColumns =
   dfNewCols.select(  'destination, $"destination", expr("destination"))
dfScalaColumns.show(2)
println(s" now go to the SQL API and create the Flight Table")
dfNewCols.createOrReplaceTempView("FlightTable")
println(s" use SQL API to select 'destination' from FlightTable")
// Immutqble data ( Spark style)  and pure transformations ( pure functions)
// Python  a = 10 then later a ="Bongo"  baaaaaaad
""" Select destination As dest From FlightTable
 |Limit 3 """.stripMargin
println(s" Create a new column appended to the dfNewCols DF")
 val dfAppendedCol = dfNewCols.selectExpr("*", "destination = origin As withinCountry")
dfAppendedCol.show(3,false)
println(s" show the data types so far")
dfAppendedCol.printSchema()
println(s""" now I want to change the DF to a DS
so I can do some calculations, using a case class ( shown outside the object) """)
val ds = dfAppendedCol.as[caseAppended]
println("s now showing the dataset")
// Dataset[caseAppended],, a DataFrame is of type Dataset[Row]
ds.show(5)
println(s"Now, just for grins, I want to change all the Booleans in the appended col to 0/1")
val dsCoded = ds.map{instance => if (instance.withinCountry) 1 else 0}
dsCoded.show(5)
println(s"getting random samples for exploratory data analysis EDA,( )maybe only want a fraction")
val seed = 42   // any fixed  allows replication of the random stream, for testing
val fraction = 0.02
val dsSample = ds.sample(true, fraction, seed)
println(s" sample with replacement (0.02)")
dsSample.show()
println(s" now do a randomSplit on the Dataset, pattern matched into a 2 component array")
val Array(d1,d2) = ds.randomSplit(Array(.4,.6), 42)
println(s" show the random Split dataframes , d1 and d2")
d1.show(5)
d2.show(3)


  }//end main()
}//end object
case class caseAppended(destination: String, origin: String,count: Long, withinCountry: Boolean)

