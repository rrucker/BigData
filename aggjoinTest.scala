/** Spark301 > aggjoinTest.scala
 * Trying out the  new Spark 3.0.1 distro
 * CH7 (aggregations) Ch 8(joins)  of the Spark guide book
 *online-retail.dataset.csv @ 45 meg
 * "InvoiceNo", StringType
 * "StockCode", StringType
 * "Description", StringType
 * "Quantity", DoubleType,
 * "InvoiceDate", TimestampType
 * "UnitPrice", DoubleType
 * "CustomerID", DoubleType,
 * "Country", StringType
 * // first few lines of the file:
 * 536386,84880,WHITE WIRE EGG HOLDER,36,12/1/2010 9:57,4.95,16029,United Kingdom
 * 536386,85099C,JUMBO  BAG BAROQUE BLACK WHITE,100,12/1/2010 9:57,1.65,16029,United Kingdom
 * 536386,85099B,JUMBO BAG RED RETROSPOT,100,12/1/2010 9:57,1.65,16029,United Kingdom
 * 536387,79321,CHILLI LIGHTS,192,12/1/2010 9:58,3.82,16029,United Kingdom
 * 536387,22780,LIGHT GARLAND BUTTERFILES PINK,192,12/1/2010 9:58,3.37,16029,United Kingdom
 *2020-10-03rr

 Note: I have NOP aesveral lines just to avoid long printouts.
Remove the "//" to see the output
 2020-10/2021-02 rr
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLImplicits}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
object aggjoinTest extends App{
Logger.getLogger("org").setLevel(Level.OFF)
type I = Int ; type S = String; type D = Double; type B = Boolean
val spark = SparkSession.builder().appName("RetailData")
.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
println(s" Spark version: ${spark.version}")

// this is pg 84 retail data by all
//there are lots of small files so use just a few partitions
val path = "/Users/rob/Desktop/retail-data/all/online-retail-dataset.csv"
val mySchema = StructType(Array(
StructField("InvoiceNo", StringType, true),
StructField("StockCode", StringType, true),
StructField("Description", StringType, true),
StructField("Quantity", DoubleType, true),
StructField("InvoiceDate", TimestampType, true), // see case class
StructField("UnitPrice", DoubleType, true),
StructField("CustomerID", DoubleType, true),
StructField("Country", StringType, true)
))
// get set to convert a DF to a DS
case class RetailData(InvoiceNo:S, StockCode:S, Description:S,
                       Quantity:D, InvoiceDate: java.sql.Timestamp,
                       UnitPrice:D, CustomerID:D,Country:S )
val rawDF = spark.read.format("csv")
.schema(mySchema).option("header","true").load(path).coalesce(5)
//Create a SQL API table ( actually a DF with a SQL API)
rawDF.createOrReplaceTempView("rawDFTable")
rawDF.show(3,false)
println(s" rawDF.count() ${rawDF.count()}")
//println(s" rawDF count() ${rawDF.select(count(*))}")
// this comes back as a Row object and I take the first( and only) component
//Note that collect() and take(n) return ROW objects from DFs, and
// Native language objects from DSs
//the syntax 'StockCode is non-standard and I won't be using it forward
val countStockCodes = rawDF.select(count('StockCode)).collect()(0)
println(s" count StockCodes $countStockCodes")
println(s" Now convert the rawDF to a Dataset and show(3)  'ds' ")
val ds = rawDF.as[RetailData]
ds.show(3)
ds.printSchema()
println(s" Now do a SQL select on count(StockCode)")
spark.sql(
"""Select count(StockCode) From rawDFTable
 """).show()
println(s" Now do a SQL select on count(*)")
spark.sql(
"""Select count(*) From rawDFTable
 """).show()
println(s" Now do a SQL select on count(1)")
spark.sql(
"""Select count(1) From rawDFTable
 """).show()
println(s" Now do a SQL select on count(Country)")
spark.sql(
"""Select count(Country) From rawDFTable
 """).show()
println(s" show the min and max of Quantity ")
val dfMinMax =ds.select(min('Quantity), max('Quantity))
dfMinMax.show(1)
println(s" take(3) from ds and println as a for each")
val t2 = ds.take(3)
t2.foreach(e => println(e))
//println(s" ds.first ${ds.first}")
println(s" agg(collect_set .... gives bunches of output  ")
println(s" agg( collect_set('Country), collect_list('Country), take(1)")
val dsAggCollect = ds.agg( collect_set('Country), collect_list('Country)).show()
//val t3 = dsAggCollect.take(1)
//t3.foreach(println)

 val UDFExampleDF = spark.range(4).toDF("nr")
 UDFExampleDF.show()
 def p3(x: D):D = math.pow(x,3)
 //println(s" register my power 3 fn ")
val p3udf = udf(p3(_:D):D)
 UDFExampleDF.select(p3udf(col("nr"))).show()

val Person = Seq(
 (0, "bill", 0, Seq(100)),
 (1, "matei", 1, Seq(500,  250, 100)),
 (2, "mike", 1, Seq(250, 100)),
).toDF("personId", "name", "graduateProgramId", "sparkStatusSeq")
 Person.show(3, false)
val GraduateProgram = Seq(
 (0,"Masters","School of Information", "UC Berkeley"),
 (2,"Masters","EECS", "UC Berkeley"),
 (1,"Ph.D.","EECS", "UC Berkeley")
).toDF("graduateProgramId","degree","department","school")
 // maybe status here is highest status attained?
 GraduateProgram.show(3,false)
val SparkStatus = Seq(
 (500, "Vice President"),
 (250, "PMC Member"),
 (100, "Contributor")
).toDF("sparkStatusHighest","companyRank")
 SparkStatus.show(3, false)
 Person.createOrReplaceTempView("person")
 GraduateProgram.createOrReplaceTempView("graduateProgram")
 // I renamed some parts of these tables for clarity 10-06 rr
 SparkStatus.createOrReplaceTempView("sparkStatus")
/* person              graduateProgram                  sparkStatus
    personId            graduateProgramId                 sparkStatusHighest
    graduateProgramId   degree                            companyRank
    sparkStatusSeq      department
                         school
*/
 // Inner joins
 println(s" inner joins on Person.graduatProgramId === GraduateProgram.graduateProgramId")
val joinExpression =
 Person.col("graduateProgramId") === GraduateProgram.col("graduateProgramId")
Person.join(GraduateProgram, joinExpression).show()
 println(s" outer full join , using same key matching as before")
 val joinType = "outer"
 Person.join(GraduateProgram, joinExpression, joinType).show()
println(s" left outer join, with graduateProgram as left DF")
 GraduateProgram.join(Person, joinExpression, "left_outer").show(4,false)
println(s" join type left_semi pe4rson on the left")
val joinType3 = "left_semi"
 GraduateProgram.join(Person, joinExpression, joinType3).show(4,false)
println(s" left anti-join, with person on the left")
 GraduateProgram.join(Person, joinExpression, "left_anti").show(4,false)

 /*
val joinExpression2 =
 person.selectExpr(person('graduateProgramId )===
                               graduateProgram('graduateProgramId))
 person.join(graduateProgram, joinExpression2).show()
*/



}//end object
