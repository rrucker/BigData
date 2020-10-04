/** Spark301 > stickDecisionTree.scala
 * I coded up the stick figure diagram of Data Science Ch 3
 * fig 3-2, I went from left to right
 * where Yes = 1, No = 0 ; bodytype { rectangle = 1, ellipse = 0}
 * headType{square = 1, round = 0 };  color { white = 0, grey = 1}
 * I typed this in as a file and read it in as a textFile
 * 0 0 1 0  no, ellipse, square, white
 * 1 0 0 1
 * 1 1 1 0
 * 1 1 1 0
 * 1 1 1 0
 * 0 1 0 1 no, rectangle, round, grey
 * 1 1 1 0
 * 0 0 1 0
 * 0 0 1 0
 * 1 1 1 0
 * 0 0 1 0
 * 1 0 0 0 yes, ellipse, round, white
************ Result of the code execution below************
 * Spark version: 3.0.1
 * rawStrings.first 0 0 1 0, a string with 4 tokens
 * learned  classification tree and model toDebugString
 *
 * model.toDebugString DecisionTreeModel classifier of depth 2 with 7 nodes
 * If (feature 0 <= 0.5)
 * If (feature 1 <= 0.5)
 * Predict: 1.0
 * Else (feature 1 > 0.5)
 * Predict: 0.0
 * Else (feature 0 > 0.5)
 * If (feature 1 <= 0.5)
 * Predict: 0.0
 * Else (feature 1 > 0.5)
 * Predict: 1.0
 * 2020-10-04 rr
 */
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object stickDecisionTree extends App{
Logger.getLogger("org").setLevel(Level.OFF)
type I = Int ; type S = String; type D = Double; type B = Boolean
val spark = SparkSession.builder().appName("RetailData")
.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
println(s" Spark version: ${spark.version}")
val stickPath = "/Users/rob/Desktop/stickFigure.txt"
val rawStrings = spark.sparkContext.textFile(stickPath)
println(s" rawStrings.first ${rawStrings.first}, a string with 4 tokens" )
// need all values to be doubles, zero index is label, others are features
val trainingData = rawStrings.map{ line =>
val Array(lab,bt,ht,col) = line.split("""\s+""")
LabeledPoint(lab.toDouble,Vectors.dense( bt.toDouble, ht.toDouble, col.toDouble))
}//end map
//set up decision tree algorithm parameters
// the categorical FeaturesInfo is required even if empty
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "entropy"     // could use "gini"
val maxDepth = 5
val maxBins = 5
// this is a classification, not a regression
val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
impurity, maxDepth, maxBins)
println(
s""" learned  classification tree and model toDebugString \n """)
println(s" model.toDebugString ${model.toDebugString} ")

}//end object
/*   Extra documentation from Spark APIs
// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini" /// I used entropy as impurity  class
val maxDepth = 5
val maxBins = 32
trainClassifier(RDD<LabeledPoint> input,
int numClasses, scala.collection.immutable.Map<Object,
Object> categoricalFeaturesInfo,
String impurity, int maxDepth, int maxBins)
Method to train a decision tree model for binary or multiclass classification.
*/
