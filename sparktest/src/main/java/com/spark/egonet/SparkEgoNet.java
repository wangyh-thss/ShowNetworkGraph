package com.spark.egonet;

import com.spark.utils.EgoUtils;

import com.spark.utils.FileWriter;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by stefanie on 12/20/16.
 */

public class SparkEgoNet {

    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws Exception {

        //String inputFileName = args[0];
        //String outputFileName = args[1];

        String inputFileName = "src/main/resources/facebook_combined.txt";
        //String inputFileName = "src/main/resources/test_data.txt";
        //String outputFileName = "src/main/resources/twitter_result.txt";
        String outputFileName = "src/main/resources/facebook_result.txt";
        String recommandFileName = "src/main/resources/facebook_recommand.txt";

        //HadoopFileWriter hfw = new HadoopFileWriter(outputFileName);


        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf().setAppName("SparkEgoNet");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(inputFileName, 1);
        //JavaRDD<String> lines = sc.textFile("src/main/resources/twitter_combined.txt", 1);
        //JavaRDD<String> lines = sc.textFile("src/main/resources/gplus_combined_test.txt", 1);

        JavaPairRDD<String, String> edges = lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

            public Iterator<Tuple2<String, String>> call(String s) throws Exception {

                EgoUtils eu = new EgoUtils();

                String[] temp = SPACE.split(s);
                ArrayList<Tuple2<String, String>> edgeList = new ArrayList<Tuple2<String, String>>();
                Tuple2<String, String> newEdge = eu.makeTuple2(temp[0], temp[1]);
                if (!edgeList.contains(newEdge)) {
                    edgeList.add(newEdge);
                }
                return edgeList.iterator();
            }
        });

//        // test edges
//        List<Tuple2<String, String>> edgeList = edges.collect();
//        for (Tuple2<String, String> e: edgeList) {
//            System.out.println("Edge: (" + e._1 + ", " + e._2 + ")");
//        }

        final List<Tuple2<String, String>> tempEdges = edges.collect();
        ArrayList allEdges = new ArrayList(tempEdges);
        final Broadcast<ArrayList> broadcastAllEdges = sc.broadcast(allEdges);

        // get each points number of the ego net
        JavaPairRDD<String, Integer> ones = edges.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, String>, String, Integer>() {

        	public Iterator<Tuple2<String, Integer>> call(Tuple2<String, String> s) throws Exception{

        	    String point1 = s._1();
        	    String point2 = s._2();
        	    ArrayList<Tuple2<String, Integer>> pointList = new ArrayList<Tuple2<String, Integer>>();
                pointList.add(new Tuple2<String, Integer>(point1, 1));
                pointList.add(new Tuple2<String, Integer>(point2, 1));

                return pointList.iterator();
            }
        });

        JavaPairRDD<String, Integer> pointDict = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

        	public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        long rou = (long)Math.floor(Math.sqrt(pointDict.count()));
        if (rou < 3) {
            rou = 3;
        }
        final Broadcast<Long> broadcastRou = sc.broadcast(rou);


        JavaPairRDD<Tuple3, ArrayList<Tuple2<String, String>>> mapResult = edges.mapToPair(
                new PairFunction<Tuple2<String, String>, Tuple3, ArrayList<Tuple2<String, String>>>() {

            public Tuple2<Tuple3, ArrayList<Tuple2<String, String>>> call(Tuple2<String, String> s) {

                EgoUtils eu = new EgoUtils();
                long rou = broadcastRou.value();

                long i = eu.hashNode(s._1(), rou);
                long j = eu.hashNode(s._2(), rou);

                if (i == j) {
                    for (long z = 0L; z < rou; z++) {
                        if (z != i) {
                            for (long w = 0L; w < rou; w++) {
                                if (w != i && w != z) {
                                    Tuple3<Long, Long, Long> key = eu.sortTuple3(i, z, w);
                                    ArrayList<Tuple2<String, String>> value = eu.getTuple2List(s._1(), s._2());
                                    return new Tuple2(key, value);
                                }
                            }
                        }
                    }
                } else {
                    for (long z = 0; z < rou; z++) {
                        if (z != i && z != j) {
                            Tuple3<Long, Long, Long> key = eu.sortTuple3(i, j, z);
                            ArrayList<Tuple2<String, String>> value = eu.getTuple2List(s._1(), s._2());
                            return new Tuple2(key, value);
                        }
                    }
                }
                return null;
            }
        });

//        //test map result
//        List<Tuple2<Tuple3, ArrayList<Tuple2<String, String>>>> mresult = mapResult.collect();
//        for (Tuple2<Tuple3, ArrayList<Tuple2<String, String>>> r : mresult) {
//            Tuple3 egoNode = r._1();
//            ArrayList<Tuple2<String, String>> right = r._2();
//            System.out.println("ego: (" + egoNode._1() + ", " + egoNode._2() + ", " + egoNode._3() + ")");
//            for (Tuple2<String, String> a : right) {
//                System.out.println("edge: (" + a._1() + "," + a._2() + ")");
//            }
//        }

        JavaPairRDD<Tuple3, ArrayList<Tuple2<String, String>>> reduceResult = mapResult.reduceByKey(
                new Function2<ArrayList<Tuple2<String, String>>, ArrayList<Tuple2<String, String>>, ArrayList<Tuple2<String, String>>>() {

            public ArrayList<Tuple2<String, String>> call(ArrayList<Tuple2<String, String>> a1, ArrayList<Tuple2<String, String>> a2) {
                ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>(a1);
                result.addAll(a2);
                return result;
            }
        });

//        //test reduce result
//        List<Tuple2<Tuple3, ArrayList<Tuple2<String, String>>>> rresult = reduceResult.collect();
//        for (Tuple2<Tuple3, ArrayList<Tuple2<String, String>>> r : rresult) {
//            Tuple3 egoNode = r._1();
//            ArrayList<Tuple2<String, String>> right = r._2();
//            System.out.println("ego: (" + egoNode._1() + ", " + egoNode._2() + ", " + egoNode._3() + ")");
//            for (Tuple2<String, String> a : right) {
//                System.out.println("edge: (" + a._1() + "," + a._2() + ")");
//            }
//        }

        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> mapResult2 = reduceResult.flatMapToPair(
                new PairFlatMapFunction<
                        Tuple2<Tuple3, ArrayList<Tuple2<String, String>>>, String, ArrayList<Tuple2<String, String>>>() {

            public Iterator<Tuple2<String, ArrayList<Tuple2<String, String>>>> call(Tuple2<Tuple3, ArrayList<Tuple2<String, String>>> t)
                    throws Exception {

                ArrayList<Tuple2<String, String>> edgesSet = t._2();
                ArrayList<Tuple2<String, String>> allEdges = broadcastAllEdges.value();
                //System.out.println(allEdges.size());
                FastEgoNetAlgorithm algorithm = new FastEgoNetAlgorithm(allEdges, edgesSet);
                Map<String, ArrayList<Tuple2<String, String>>> tempResult = algorithm.runAlgorithm();

                ArrayList<Tuple2<String, ArrayList<Tuple2<String, String>>>> result =
                        new ArrayList<Tuple2<String, ArrayList<Tuple2<String, String>>>>();

                Iterator<String> iter = tempResult.keySet().iterator();

                while (iter.hasNext()) {
                    String key = iter.next();
                    result.add(new Tuple2<String, ArrayList<Tuple2<String, String>>>(key, tempResult.get(key)));
                }

                return result.iterator();
            }
        });

//        //test mapresult2
//        List<Tuple2<String, ArrayList<Tuple2<String, String>>>> result = mapResult2.collect();
//        for (Tuple2<String, ArrayList<Tuple2<String, String>>> r : result) {
//            String egoNode = r._1();
//            ArrayList<Tuple2<String, String>> right = r._2();
//            System.out.println("--EgoNode: " + egoNode + "--");
//            for (Tuple2<String, String> a : right) {
//                System.out.println("edge: (" + a._1() + "," + a._2() + ")");
//            }
//        }

        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> reduceResult2 = mapResult2.reduceByKey(
                new Function2<ArrayList<Tuple2<String, String>>, ArrayList<Tuple2<String, String>>, ArrayList<Tuple2<String, String>>>() {

            public ArrayList<Tuple2<String, String>> call(ArrayList<Tuple2<String, String>> a1, ArrayList<Tuple2<String, String>> a2) {

                ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>(a1);
                for (Tuple2<String, String> t : a2) {
                    if (!result.contains(t)) {
                        result.add(t);
                    }
                }
                return result;
            }
        });

        long end = System.currentTimeMillis();

        FileWriter fw = new FileWriter(outputFileName);
        fw.writeLineToFile("program use time: " + Long.toString(end-start) + "s\n");

        //output result to txt file
        List<Tuple2<String, ArrayList<Tuple2<String, String>>>> result = reduceResult2.collect();

        for (Tuple2<String, ArrayList<Tuple2<String, String>>> r : result) {
            String egoNode = r._1();
            ArrayList<Tuple2<String, String>> right = r._2();

            RecommandAlgorithm recommand = new RecommandAlgorithm(egoNode, right);

            fw.writeLineToFile("EgoNode: " + egoNode + '\n');
            for (Tuple2<String, String> a : right) {
                fw.writeLineToFile("(" + a._1() + ", " + a._2() + "),");
            }
            fw.writeLineToFile("\n");
        }
        fw.closeFileWriter();

        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> mapResult3 =
                reduceResult2.mapToPair(new PairFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, ArrayList<Tuple2<String, String>>>() {

                    public Tuple2<String, ArrayList<Tuple2<String, String>>> call(
                            Tuple2<String, ArrayList<Tuple2<String, String>>> t) throws Exception {

                        String egoNode = t._1();
                        ArrayList<Tuple2<String, String>> egoEdges = t._2();

                        RecommandAlgorithm algorithm = new RecommandAlgorithm(egoNode, egoEdges);
                        ArrayList<Tuple2<String, String>> result = algorithm.runAlgorithm();

                        return new Tuple2<String, ArrayList<Tuple2<String, String>>>(egoNode, result);
                    }
                });


        FileWriter rf = new FileWriter(recommandFileName);

        //output result to txt file
        List<Tuple2<String, ArrayList<Tuple2<String, String>>>> result3 = mapResult3.collect();

        for (Tuple2<String, ArrayList<Tuple2<String, String>>> r : result3) {

            String egoNode = r._1();
            ArrayList<Tuple2<String, String>> right = r._2();

            RecommandAlgorithm recommand = new RecommandAlgorithm(egoNode, right);

            //rf.writeLineToFile("EgoNode: " + egoNode + '\n');
            for (Tuple2<String, String> a : right) {
                rf.writeLineToFile(a._1() + " " + a._2() + "\n");
            }
        }
        rf.closeFileWriter();

        //reduceResult2.saveAsTextFile(outputFileName);
        sc.stop();

    }
}
