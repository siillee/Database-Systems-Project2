package app.recommender

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
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {

    val filePath = new File(getClass.getResource(path).getFile).getPath
//    val filePath = path
    val fileLines = sc.textFile(filePath)

    val data = fileLines.map(l => {
      val tokens = l.split('|')
      val it = tokens.iterator
      val id = it.next().toInt
      val name = it.next()
      var keywords = ListBuffer[String]()
      while(it.hasNext){
        keywords += it.next()
      }
      (id, name, keywords.toList)
    })

    data.persist()
  }
}
