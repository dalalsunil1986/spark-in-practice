package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 *  Use the Ex1UserMiningTest to implement the code.
 *
 */
public class Ex1UserMining {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // Create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("User mining")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(Parse::parseJsonToTweet);

    return tweets;
  }

  /**
   *   For each user return all his tweets
   */
  public JavaPairRDD<String, Iterable<Tweet>> tweetsByUser() {
    JavaRDD<Tweet> tweets = loadData();

    // Hint: the Spark API provides a groupBy method
    JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = tweets.groupBy(Tweet::getUser);

    return tweetsByUser;
  }

  /**
   *  Compute the number of tweets by user
   */
  public JavaPairRDD<String, Integer> tweetByUserNumber() {
    JavaRDD<Tweet> tweets = loadData();

    // Hint: think about what you did in the wordcount example
    JavaPairRDD<String, Integer> count =
            tweets.mapToPair(tweet -> new Tuple2<>(tweet.getUser(), 1))
            .reduceByKey(Integer::sum);

    return count;
  }

}
