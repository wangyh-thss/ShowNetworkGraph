package com.spark.test;
/**
 * Created by stefanie on 12/20/16.
 */

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile("src/main/resources/gplus_combined_test.txt", 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(SPACE.split(s)).iterator();
			}

        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

        	public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

        	public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<String> wordsList = words.collect();
        for (String w : wordsList) {
            System.out.println(w);
        }
        ctx.stop();
        //List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?,?> EgoUtils : output) {
//            System.out.println(EgoUtils._1() + ": " + EgoUtils._2());
//        }
//        ctx.stop();
    }
}
