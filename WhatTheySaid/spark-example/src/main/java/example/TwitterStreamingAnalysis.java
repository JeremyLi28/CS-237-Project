package example;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.auth.Authorization;
import twitter4j.auth.AccessToken;

import org.apache.spark.streaming.twitter.*;

//import org.json.simple.JSONObject;
import org.json.*;

public class TwitterStreamingAnalysis {
	private static final String consumerKey = "4yn6CjzxHwKmDyY6TJTvrpCYP";
	private static final String consumerSecret = "gWlgiHbM1q7tL5k8LLQ7vaCDYboSZQt4EKXMkHNq1ZAZoz7Z4C";
	private static final String accessToken = "734546468573347840-rkY68hk9rQ2m2CRF3ltyqYwoJshJjz2";
	private static final String accessTokenSecret = "EkoziulDpZ8VePBEZvhBWPsApj9KA53HMvMaxUNkOAd2p";
	
	private static final Set<String> posWords = new HashSet<String>();
	private static final Set<String> negWords = new HashSet<String>();
	private static final Set<String> stopWords = new HashSet<String>();
	
	private static transient JavaReceiverInputDStream<twitter4j.Status> receiver;
	private static transient JavaDStream<JSONObject> json;
	private static JavaDStream<String> serialized;
	
	public static void main(String args[]) throws Exception {
		readWords("pos-words.txt", posWords);
		readWords("neg-words.txt", negWords);
		readWords("stop-words.txt", stopWords);
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStreammingExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
				
		Authorization oAuth = getTwitterOAuth();
		
		receiver = TwitterUtils.createStream(jssc, oAuth);
		
		json = receiver.filter(x -> (x.getText().length() > 0))
				.map(x -> twitterToJSON(x))
				.map(x -> computeScore(x));
		
		serialized = json.map(x -> x.toString());
		
		serialized.print();
		
		jssc.start();
		jssc.awaitTermination();
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
