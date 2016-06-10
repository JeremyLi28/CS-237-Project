package example;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.JSONObject;

import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class SearchTwitterAnalysis {
	private static Twitter twitter;
	
	private static final Set<String> posWords = new HashSet<String>();
	private static final Set<String> negWords = new HashSet<String>();
	private static final Set<String> stopWords = new HashSet<String>();
	
	/**
	 * Set:
	 * latitude, longtitude, radius, count
	 * Result is scored in {@code scores}
	 * @param args
	 * @throws TwitterException
	 */
	public static void main(String[] args) throws TwitterException {
		setUpTwitter();
		
		double latitude = 33.6839;
		double longtitude = -117.7947;
		double radius = 10;
		int count = 10;
		List<Status> tweets = searchTwitter(latitude, longtitude, radius, count);
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SearchTwitterApplication");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Status> tweetsRdd = sc.parallelize(tweets);
		
		List<String> scores = computeSentiment(tweetsRdd);
		for (String score: scores) {
			System.out.println(score);
		}
		
		sc.close();
	}
	
	private static void setUpTwitter() {
		String accessToken = "734546468573347840-rkY68hk9rQ2m2CRF3ltyqYwoJshJjz2";
		String accessTokenSecret = "EkoziulDpZ8VePBEZvhBWPsApj9KA53HMvMaxUNkOAd2p";
		String consumerKey = "4yn6CjzxHwKmDyY6TJTvrpCYP";
		String consumerSecret = "gWlgiHbM1q7tL5k8LLQ7vaCDYboSZQt4EKXMkHNq1ZAZoz7Z4C";
		ConfigurationBuilder build = new ConfigurationBuilder();
	    
	    build.setOAuthAccessToken(accessToken);
	    build.setOAuthAccessTokenSecret(accessTokenSecret);
	    build.setOAuthConsumerKey(consumerKey);
	    build.setOAuthConsumerSecret(consumerSecret);
	    OAuthAuthorization auth = new OAuthAuthorization(build.build());
	    twitter = new TwitterFactory().getInstance(auth);
	}
	
	private static List<Status> searchTwitter(double latitude, double longtitude, double radius, int count) throws TwitterException {
		Query query = new Query();
		query.setGeoCode(new GeoLocation(latitude, longtitude), radius, Query.MILES);
		query.setCount(count);
		System.out.println(query.toString());
		
		QueryResult queryRes = twitter.search(query);
		List<Status> tweets = queryRes.getTweets();
//		for (Status tweet : tweets) {
//            System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
//        }
		return tweets;
	}
	
	private static List<String> computeSentiment(JavaRDD<Status> tweetsRdd) {
		readWords("pos-words.txt", posWords);
		readWords("neg-words.txt", negWords);
		readWords("stop-words.txt", stopWords);
		
		JavaRDD<JSONObject> json = tweetsRdd.filter(x -> (x.getText().length() > 0))
				.map(x -> twitterToJSON(x))
				.map(x -> computeScore(x));
		
		JavaRDD<String> serialized = json.map(x -> x.toString());
		
//		serialized.foreach(new VoidFunction<String>() {
//			public void call(String str) {
//				System.out.println(str);
//			}
//		});
		
		List<String> scores = serialized.collect();
		return scores;
	}
	
	private static JSONObject computeScore(JSONObject json) {
		String[] strs = json.get("text").toString().split(" ");
		double score = 0;
		
		for (String str: strs) {
			if (stopWords.contains(str)) {
				continue;
			} else if (posWords.contains(str)) {
				score ++;
			} else if (negWords.contains(str)) {
				score --;
			}
		}
		json.append("score", Double.toString(score/strs.length));
		return json;
	}
	
	private static JSONObject twitterToJSON(twitter4j.Status twitter) {
		JSONObject json = new JSONObject();
		//add all fields we need
		//text
		json.append("text", twitter.getText().toLowerCase().replaceAll("[^0-9a-zA-Z\\s]+", ""));
		//geoLocation
		if (twitter.getGeoLocation() == null) {
			json.append("location", null);
		} else {
			json.append("location", Double.toString(twitter.getGeoLocation().getLatitude()));
			json.append("location", Double.toString(twitter.getGeoLocation().getLongitude()));
		}
		
		return json;
	}
	
	private static void readWords(String filename, Set<String> words) {
		ClassLoader classLoader = TwitterStreamingAnalysis.class.getClassLoader();
		File file = new File(classLoader.getResource(filename).getFile());
		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				words.add(line);
			}

			scanner.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
