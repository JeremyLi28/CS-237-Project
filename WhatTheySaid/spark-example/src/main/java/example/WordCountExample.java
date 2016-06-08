package example;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

import org.apache.spark.SparkConf;

public class WordCountExample {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//initiate
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFile = sc.textFile("data.txt");
		
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); } 
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s,1);}
		});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) { return a+b; }
		});
		
//		counts.saveAsTextFile("Counts.txt");
		counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			public void call(Tuple2<String, Integer> count) throws Exception {
				System.out.println(count._1() + " " + count._2());
			}
		});
		
		sc.close();
	}
}
