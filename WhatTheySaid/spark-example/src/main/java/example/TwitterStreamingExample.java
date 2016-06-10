package example;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.auth.Authorization;
import twitter4j.GeoLocation;
import twitter4j.auth.AccessToken;

import org.apache.spark.streaming.twitter.*;

public class TwitterStreamingExample {
	private static final String consumerKey = "ntNC6bOzZ71az727bK18TpvmH";
	private static final String consumerSecret = "vWjc4arA0XHdyIl4Q6IBJadV34RK9IHujPNTq3NatGgtOq5vpj";
	private static final String accessToken = "2234236346-WFAszADHQd8BrqEJWXzJvhRebSaayGIM7M5V72A";
	private static final String accessTokenSecret = "wfxeD5GjjdK4LS9wdCOZwtmlhz1gWvzmVW8fsq2ZVQdJV";
	
	private static transient JavaReceiverInputDStream<twitter4j.Status> receiver;
	private static transient JavaDStream<twitter4j.Status> hasTrump;
	private static transient JavaDStream<String> hasTrumpTweetText;
	
	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
		//place
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStreammingExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
				
		Authorization oAuth = getTwitterOAuth();
		
		receiver = TwitterUtils.createStream(jssc, oAuth);
		
		hasTrump = receiver.filter(new Function<twitter4j.Status, Boolean>() {
			public Boolean call(twitter4j.Status tweet) {
				return tweet.getText().contains("Trump");
			}
		});		
		
		hasTrumpTweetText = hasTrump.map(new Function<twitter4j.Status, String>() {
			public String call(twitter4j.Status tweet) {
				GeoLocation geo = tweet.getGeoLocation();
				if (geo == null) {
					return "No location.";
				} else {
					return tweet.getText() + " " + Double.toString(geo.getLatitude()) + " " + Double.toString(geo.getLongitude());
				}
			}
		});
		
		hasTrumpTweetText.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) {
				if (rdd.count() > 0) {
					rdd.saveAsTextFile("LocationsOfTweets");
				}
			}
		});
		
//		JavaDStream<Integer> hasTrumpSingleTweetCount = hasTrump.map(new Function<twitter4j.Status, Integer>() {
//			public Integer call(twitter4j.Status tweet) {
//				return 1;
//			}
//		});
//		
//		JavaDStream<Integer> hasTrumpTotalTweetCount = singleTweetCount.reduce(new Function2<Integer, Integer, Integer>() {
//			public Integer call(Integer a, Integer b) {
//				return a + b;
//			}
//		});
//		
//		totalTweetCount.print();
		
		jssc.start();
		jssc.awaitTermination();
		
	}
	
	private static Authorization getTwitterOAuth() {
		ConfigurationBuilder  conf = new ConfigurationBuilder();
		conf.setDebugEnabled(true)
			.setOAuthConsumerKey(consumerKey)
			.setOAuthConsumerSecret(consumerSecret)
			.setOAuthAccessToken(accessToken)
			.setOAuthAccessTokenSecret(accessTokenSecret);
		
		OAuthAuthorization oAuth = new OAuthAuthorization(conf.build());
		oAuth.setOAuthConsumer(consumerKey, consumerSecret);
		oAuth.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
		
		return oAuth;
	}
}
