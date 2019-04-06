package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *  Buildind a hashtag search engine
 *
 *  The goal is to build an inverted index. An inverted index is the data structure used to build search engines.
 *
 *  How does it work?
 *
 *  Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
 *  The inverted index that you must return should be a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
 *
 */
public class Ex4InvertedIndex {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Inverted index")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  public Map<String, Iterable<Tweet>> invertedIndex() {
    JavaRDD<Tweet> tweets = loadData();

    // for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Hint: see the flatMapToPair method
    JavaPairRDD<String, Tweet> pairs = tweets.flatMapToPair(tweet -> {
      List results = new ArrayList();
      List<String> hashtags = new ArrayList<>();
      List<String> words = Arrays.asList(tweet.getText().split(" "));

      // Add all found hashtags to hashtags ArrayList
      for (String word: words) {
        if(word.startsWith("#") && word.length() > 1) {
          hashtags.add(word);
        }
      }

      // Assign current tweet to all found hashtags in text
      for (String hashtag: hashtags) {
        Tuple2<String, Tweet> result = new Tuple2<>(hashtag, tweet);
        results.add(result);
      }

      return results; // (hashtag, tweet)
    });


    // We want to group the tweets by hashtag
    JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = pairs.groupByKey(); // (hashtag, [tweet, tweet])

    // Then return the inverted index (= a map structure)
    Map<String, Iterable<Tweet>> map = tweetsByHashtag.collectAsMap();

    return map;
  }

}
