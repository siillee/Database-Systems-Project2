package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]] = null
  var histogram : mutable.Map[IndexedSeq[Int], Int] = mutable.Map[IndexedSeq[Int], Int]()
  var signatureCount : Long = 0
  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {

    val frequent = histogram.filter(el => el._2 / signatureCount > 0.01).keys.toList
    val data = lshIndex.getBuckets()
    cache = sc.broadcast(data.filter(el => frequent.contains(el._1)).collect().toMap)
    histogram.clear()
    signatureCount = 0
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {

    cache = ext
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {

    val hashed = lshIndex.hash(queries)

    if (cache == null){
      return (null, hashed)
    }
    // Updating the histogram
    signatureCount = signatureCount + hashed.map(el => el._1).distinct().count()
    hashed.foreach(el => {
      if (!histogram.contains(el._1)) {
       histogram += ((el._1, 0))
      }
      histogram(el._1) = histogram(el._1) + 1
    })

    val cacheMap = cache.value

    val hit = hashed.filter(el => cacheMap.contains(el._1))
                    .map(el => (el._2, cacheMap(el._1)))

    val missed = hashed.filter(el => !cacheMap.contains(el._1))

    (hit, missed)
  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {

    val cacheLookupResult = cacheLookup(queries)
    val missedLookup = lshIndex.lookup(cacheLookupResult._2)

    if (cacheLookupResult._1 == null){
      return missedLookup.map(el => (el._2, el._3))
    }

    cacheLookupResult._1.union(missedLookup.map(el => (el._2, el._3)))
  }
}
