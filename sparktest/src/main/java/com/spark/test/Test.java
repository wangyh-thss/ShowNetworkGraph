package com.spark.test;

import com.spark.egonet.RecommandAlgorithm;
import com.spark.utils.SubEgoNet;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by stefanie on 12/20/16.
 */
public class Test {

    public static void main(String[] args) throws Exception {

        ArrayList<Tuple2<String, String>> testEdges = new ArrayList<Tuple2<String, String>>();
        testEdges.add(new Tuple2<String, String>("A", "B"));
        testEdges.add(new Tuple2<String, String>("A", "D"));
        testEdges.add(new Tuple2<String, String>("A", "H"));
        testEdges.add(new Tuple2<String, String>("A", "I"));
        testEdges.add(new Tuple2<String, String>("B", "C"));
        testEdges.add(new Tuple2<String, String>("D", "E"));
        testEdges.add(new Tuple2<String, String>("D", "F"));
        testEdges.add(new Tuple2<String, String>("E", "G"));
        testEdges.add(new Tuple2<String, String>("E", "H"));
        testEdges.add(new Tuple2<String, String>("F", "G"));
        testEdges.add(new Tuple2<String, String>("I", "J"));
        testEdges.add(new Tuple2<String, String>("I", "K"));

        RecommandAlgorithm relation = new RecommandAlgorithm("A", testEdges);
        ArrayList<Tuple2<String, String>> result = relation.runAlgorithm();

        for (Tuple2<String, String> t : result) {
            System.out.println("(" + t._1() + "," + t._2() + ")");
        }
//        //ArrayList<SubEgoNet> result = relation.findSubEgoNet();
//
//        int i = 0;
//        for (SubEgoNet net : result) {
//
//            System.out.println("-----The No." + i + " ego net-----");
////            ArrayList<Tuple2<String, String>> edges = net.getEdgeList();
////            for (Tuple2<String, String> t : edges) {
////                System.out.println("(" + t._1() + "," + t._2() + ")");
////            }
////            ArrayList<String> pointList = net.getPointList();
////            for (String p : pointList) {
////                System.out.println(p);
////            }
//            ArrayList<Tuple2<String, String>> recommandEdges = net.r();
//            for (Tuple2<String, String> t : recommandEdges) {
//                System.out.println("(" + t._1() + "," + t._2() + ")");
//            }
//            i++;
//        }

//        Map<String, ArrayList<Tuple2<String, String>>> result = algorithm.runAlgorithm();
//        for (String key : result.keySet()) {
//            System.out.println("---" + key + "---");
//            for (Tuple2<String, String> t : result.get(key)) {
//                System.out.println("(" + t._1() + "," + t._2() + ")");
//            }
//        }
    }
}
