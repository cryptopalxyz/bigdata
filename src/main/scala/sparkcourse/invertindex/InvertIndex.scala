package sparkcourse.invertindex

import org.apache.spark.sql.SparkSession

object InvertIndex extends App {

  override def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("geek-spark-rdd").master("local").getOrCreate()

    //generate pair RDD
    //Array((chapter0,it is what it is), (chapter1,what is it), (chapter3,it is a banana))
    val rdd1 = spark.sparkContext.parallelize(Seq(("chapter0", "it is what it is"), ("chapter1", "what is it"), ("chapter3", "it is a banana")))
    //generate (k,array)
    //Array((chapter0,Array(it, is, what, it, is)), (chapter1,Array(what, is, it)), (chapter3,Array(it, is, a, banana)))
    val rdd2 = rdd1.mapValues(a => a.split(" "))
    //use function to flat map the array
    //Array((chapter0,it), (chapter0,is), (chapter0,what), (chapter0,it), (chapter0,is), (chapter1,what), (chapter1,is), (chapter1,it), (chapter3,it), (chapter3,is))
    //Array[((String, Array[String]), Int)] = Array(((chapter0,Array(it, is, what, it, is)),1), ((chapter1,Array(what, is, it)),1), ((chapter3,Array(it, is, a, banana)),1))
    val rdd3 = rdd2.flatMap {
      case (k, v) => v.map(vx => (k, vx))
    }

    val rdd4 = rdd3.map(a => (a, 1)).reduceByKey(_ + _)

    val rdd5 = rdd4.map(a => (a._1._2, (a._1._1, a._2))).mapValues(a => List(a))
    //Array((a,List((chapter3,1))), (what,List((chapter1,1), (chapter0,1))), (banana,List((chapter3,1))), (is,List((chapter3,1), (chapter1,1), (chapter0,2))), (it,List((chapter3,1), (chapter0,2), (chapter1,1))))
    val rdd6 = rdd5.reduceByKey(_ ++ _)

  }

}
