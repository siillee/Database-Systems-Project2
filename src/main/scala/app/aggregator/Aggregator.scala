package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state : RDD[(Int, String, (Double, Int), List[String])] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {

    val ratingsId = ratings.map(el => el._2)
    val titleId = title.map(el => el._1)
    val zeroRatingsTmp = titleId.subtract(ratingsId)
    val zeroRatings =  zeroRatingsTmp.map(el => (el, (0.0, 0)))

    val aggregated = new PairRDDFunctions(ratings.groupBy(el => el._2))
      .aggregateByKey((0.0, 0))((a, b) => {
        var sum = 0.0
        b.foreach(x => sum = sum + x._4)
        (a._1 + sum, a._2 + b.size)
      }, (c, d) => (c._1 + d._1, c._2 + d._2)).union(zeroRatings)

    state = aggregated.join(title.keyBy(el => el._1)).map(el => (el._1, el._2._2._2, el._2._1, el._2._2._3))
    state.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {

    state.map(el => {
      if (el._3._2 == 0) {
        (el._2, 0.0)
      }else {
        (el._2, el._3._1 / el._3._2)
      }
    })
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = {

    val filtered = state.filter(el => keywords.forall(el._4.contains))
    if (filtered.isEmpty()){
      return -1
    }

    val aggregated = filtered.aggregate((0.0, 0))((a, b) => {
      var count = 0
      var sum = 0.0
      if (b._3._1 != 0.0){
        sum = sum + b._3._1 / b._3._2
        count = count + 1
      }
      (a._1 + sum, a._2 + count)
    }, (a, b) => (a._1 + b._1, a._2 + b._2))

    aggregated._1 / aggregated._2
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {

    state.unpersist()
    val delta = delta_.groupBy(el => el._2)
    state = state.map(el => {
      var deltaSum = 0.0
      var deltaCount = 0
      if (delta.contains(el._1)) {
        delta(el._1).foreach(r => {
          if (r._3.nonEmpty) {
            deltaSum = deltaSum + r._4 - r._3.get
          }else {
            deltaSum = deltaSum + r._4
            deltaCount = deltaCount + 1
          }
        })
      }
      (el._1, el._2, (el._3._1 + deltaSum, el._3._2 + deltaCount), el._4)
    })

    state.persist()
  }
}
