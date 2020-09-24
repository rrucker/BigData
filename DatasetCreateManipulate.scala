

/**DataFrameDatasetTutorial2020  (project)
 * DatasetCreateManipulate.scala
 * Focus on Datasets , creation and manipulation
 * 1. Datasets are the foundation of the Structured APIs
 * 2. I show successive creation of DataFrames and then creation of
 *     Datasets
 *     RDDs are shown at the end of the code
 * 2020-09-15 r.r
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
object DatasetCreateManipulate extends App{
Logger.getLogger("org").setLevel(Level.OFF)
type I = Int ; type S = String; type D = Double
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession
.builder().appName("DatasetCreateManiplate")
.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
case class Data1(id:Long, age: I, status: D, region : S)
println(s" 'testSeq', a simple Scala Sequence of 4-tuples ( no names or types ")
val testSeq= Seq (
(1, 45, 68.0,"A"),
(2, 65, 73.6,"B"),
(3, 85, 82.1,"B"),
(4, 28, 57.0,"B"),
(5, 33, 61.0, "A")
)
println(s"transform testSeq.toDF(), =df1,before column names or types, what could  they be?")
val df1 = testSeq.toDF()
df1.show()
println(s"df1 printSchema, given that I didn't specify names or types?")
df1.printSchema()
println(s"now,df2=testSeq.toDF(id,age,status, region)= df2,with explicit col names, no explicit types")
val df2= testSeq.toDF("id", "age", "status", "region")
df2.show()
println(s" what is df2 schema, given that I gave names but no types?")
df2.printSchema()
println(s"transform  df2 ( having col names and inferred types types), to a Case Class DS, =ds1")
println(s"val ds1 = df2.as[Data1]")
val ds1 = df2.as[Data1]
ds1.show()
s""" Note that df2 INFERRED the id type as Integer,
but case class specifies 'Long' , the inferred overrides the case class! """.stripMargin
ds1.printSchema
println(s" create ds in one pass: first--> toDF(args) then, .as[Data1] -> ds2 (same as ds1)")
val ds2 = testSeq.toDF("id","age","status","region").as[Data1]
ds2.show()
println(s" ds2 case class constructed DS, what is its schema")
ds2.printSchema()
val df4= Seq( (3, "AA"), (4,"BB"), (5, "CC") )
.toDF("nr","name")
println(s"ds4 is a Dataset using a case class")
case class DF4(nr: Int, name: String)
val ds4 = df4.as[DF4]
ds4.first()
println(s" ds4.collect() the Dataset, ds4,  and then do a foreach println")
val collection = ds4.collect()
collection.foreach(row => println (row))
println(s" spark has a method 'range' that produces a Dataset out of the box")
val r1 = spark.range(5)
println(s" show-- spark.range(5) ")
r1.show()
println(s" r1.printSchema")
r1.printSchema()
val r2 = r1.toDF("nr")
println(s"r2= r1.toDF(nr)  ")
r2.show()
println(s"r2.printSchema()")
r2.printSchema()
// ---------------------------------------- file handling ---------
// Suggested way to handle csv, txt files where each line comes in as a string
// and the string is split into components and loaded into a prepared case class
// showing multiline string, with an embedded variable
val t = "text"
val str = s"""Showing a standard way to read a $t file,
            |parse into tokens and then load into a case class to yield a Dataset
            |I created a .txt file from testSeq  and used previous case class
""".stripMargin
println(str)
//  repeated for reference from top of code block:
//  case class Data1(id:Long, age: I, status: D, region : S)
val path2 = "/Users/rob/Desktop/testSeq.txt"
val data = spark.read.textFile(path2)
val ds6 = data.map { line => {
val Array(a, b, c, d) = line.split(",")
Data1(a.trim.toLong, b.trim.toInt , c.trim.toDouble, d.trim.toString)
}
}
println(s" ds6 is now a Dataset of 'Data1' instances ")
ds6.show()
println(s" now collect the ds6 instances to the Driver program (hope not too big! ")
val collectds6 = ds6.collect()

// HOF that goes thru each element and performs a method, in this case println
collectds6.foreach ( line => println(line))
val older = collectds6.filter { instance  => instance.age  > 60}
println(s" show people over 60 as Boolean")
println(s" Now count how many people are over 60, count = ${older.size} ")
//********* Usimg RDDs
println(
s""" Now use an RDD and a Schema to create a DataFrame.
  |This approach allows more control over the structure of the DF
  |as opposed to simply using '.toDF( col1, col2 ...)'
  | ***toDF() is limited because the column type and nullable flag cannot be customized.
  | In this example, the number column is not nullable and the name column
  | is not nullable.""".stripMargin)
val seq = Seq(
Row("Brooke", 70),Row ("Denny", 31),
Row("Jules", 30),Row ("Bronco Billy" , 45),Row ("TD", 35), Row("Brooke", 25))
//val seq = Seq(
//("Brooke", 70), ("Denny", 31),
//("Jules", 30), ("Bronco Billy" , 45), ("TD", 35), ("Brooke", 25))

val mySchema  = List(
StructField("name", StringType , false),
StructField("age", IntegerType, false)
)
println(s""" NOTE: the command 'parallelize' has a synonym 'makeRDD,
note a DF is essentially the pair,  (RDD , schema)  ! """)
val dfRDD = spark.createDataFrame(
spark.sparkContext.parallelize(seq),
StructType(mySchema))
println(s" print the dfRDD schema")
dfRDD.printSchema()

println(s" Show the dfRDD from createDataFrame(rdd, schema ")
dfRDD.show(5)
val avgDF = dfRDD.groupBy("name").agg(avg("age"))
println(s" use the aggregate function to invoke the average fn---")
avgDF.show()
println(s"Parquet has become the go-to format for ML in Spark ( and in other environs too)")
val parquetPath ="/Users/rob/Desktop/dfRDD"
dfRDD.write.format("parquet").save(parquetPath)



}//end object DatasetCreateManipulate


/*
val myData = Seq(
  Row(8, "joe"),
  Row(64, "cal"),
  Row(-27, "moe")
)

val someSchema = List(
  StructField("number", IntegerType, true),
  StructField("word", StringType, true)
)

val myDF = spark.createDataFrame(
  spark.sparkContext.makeRDD(someData),
  StructType(someSchema)
)
 */







