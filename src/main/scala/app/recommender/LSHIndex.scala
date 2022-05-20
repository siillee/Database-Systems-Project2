package app.recommender

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)

  // distinct() is needed because there is a chance to have same films appear twice due to the way I keyed and joined the elements,
  // it is called first (before other transformations) in order to have better performance
  val hashed :  RDD[(List[String], (IndexedSeq[Int], (Int, String, List[String])))] =
    hash(data.map(el => el._3))
      .distinct()
      .keyBy(el => el._2)
      .map(el => (el._1, el._2._1))
      .join(data.keyBy(el => el._3))

  // cache for the partitioning by signature
  val cache : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] =
    hashed.map(el => (el._2._1, el._2._2))
      .groupBy(el => el._1)
      .map(el => (el._1, el._2.map(x => x._2).toList))
      .partitionBy(new HashPartitioner(hashed.getNumPartitions)).cache()
  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
    : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {

    cache
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {

    queries.join(getBuckets()).map(el => (el._1, el._2._1, el._2._2))
  }
}
