package com.duchessfr.spark.streaming;


import org.junit.Before;
import org.junit.Test;

public class StreamingOnTweetsTest {

  private StreamingOnTweets streaming;
  @Before
  public void init() {
    streaming = new StreamingOnTweets();
  }

  @Test
  public void printTweet() {
    // run
    streaming.tweetPrint();

    // assert
    // You must see some tweets in the console
  }

  @Test
  public void popularHashtag() {
    // run
    String result = streaming.popularHashtag();

    // assert
    System.out.println(result);
    // You should see something like that:
    // Most popular hashtag :[(1,#tlot), (1,#followme), (1,#teamfollowback)...]
  }
}
