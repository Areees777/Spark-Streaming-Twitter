import org.apache.spark.storage.StorageLevel

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Main {

  def main(args: Array[String]): Unit = {

    val consumerKey = "XXXX"
    val consumerSecret = "XXXX"
    val accessToken = "XXXX"
    val accessTokenSecret = "XXXX"
    setupLogging()

    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - PopularHashTags")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    

    val stream = TwitterUtils.createStream(ssc, Some(auth))
    val spanishTweets = stream.filter(_.getLang() == "es")

    val hashTags = spanishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topHashTags = hashTags.map(word=>(word, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topHashTags.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

}
