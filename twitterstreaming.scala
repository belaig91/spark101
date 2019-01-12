## sbt dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"


libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
###################

import org.apache.spark.streaming.twitter._
import twitter4j.auth._
import twitter4j.conf._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._

object streamTwitter {
    def main(args: Array[String]): Unit ={
      val conf = new SparkConf().setAppName("twitter streaming").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(10))
      val cb = new ConfigurationBuilder
      cb.setDebugEnabled(true).setOAuthConsumerKey("Iwkwt3A6JvwiqpG3v8kVOXDcj")
        .setOAuthConsumerSecret("pi84erqX05R4xKlu5XXKHJaNs1E2aVpxW0oxorORdkQwdmEPyw")
        .setOAuthAccessToken("1063084321437814784-8hIIalL8nBgcXsydttFvVR7ahoeoFT")
        .setOAuthAccessTokenSecret("IZABkkiDFZwoS5LdCK3RFwZxZ3ycKEG6irGgwC2HGzSjx")
      val auth = new OAuthAuthorization(cb.build)
      val tweets = TwitterUtils.createStream(ssc,Some(auth))
      val englishTweets = tweets.filter(_.getLang()=="en")
      val status = englishTweets.map(status => status.getText)
        status.print
        //ssc.checkpoint("hdfs://localhost:9000/checkpoint")
        ssc.start
        ssc.awaitTermination
}
    }

