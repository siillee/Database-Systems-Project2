package app

import app._
import app.aggregator.RatingsLoader
import app.aggregator.Aggregator
import app.recommender.{LSHIndex, NNLookupWithCache, TitlesLoader}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Main {
  def main(args: Array[String]) {
    //    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val conf = new SparkConf().setAppName("CS422-app")
    val sc = SparkContext.getOrCreate(conf)

    val ratingsLoader = new RatingsLoader(sc, "hdfs://iccluster028.iccluster.epfl.ch/cs422-data/ratings-all.csv")
    val titlesLoader = new TitlesLoader(sc, "hdfs://iccluster028.iccluster.epfl.ch/cs422-data/titles-all.csv")

    //    val aggregator = new Aggregator(sc)
    //    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    //    val res = aggregator
    //    .getResult()
    //
    //    res.saveAsTextFile("hdfs://iccluster028.iccluster.epfl.ch/user/milicevi/output_all")

    val lsh = new NNLookupWithCache(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))
    val queries = sc.makeRDD(List[List[String]](List[String]("dancer", "three-word-title")))

    lsh.build(sc)
    val res = lsh.lookup(queries)
    res.saveAsTextFile("hdfs://iccluster028.iccluster.epfl.ch/user/milicevi/output_cacheLookup8")
  }
}
