package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {

    val lineitem = new File(getClass.getResource(path).getFile).getPath
//    val fileLines = sc.textFile(lineitem)

    val fileLines = Source.fromFile(new File(lineitem)).getLines()

    val data = fileLines.map(l => {
      val tokens = l.split('|')
      val idu = tokens(0).toInt
      val idt = tokens(1).toInt
      val rating = tokens(2).toDouble
      val timestamp = tokens(3).toInt
      (idu, idt, Option.empty[Double], rating, timestamp)
    })

    val rdd = sc.makeRDD(data.toSeq)
    rdd.persist()

//    data.persist()

  }
}
