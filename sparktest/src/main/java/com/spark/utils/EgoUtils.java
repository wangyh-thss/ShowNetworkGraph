package com.spark.utils;

import javafx.util.Pair;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.lang.String;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by stefanie on 12/20/16.
 */
public class EgoUtils {

    public long hashNode(String node, Long seed) {

        long result = node.hashCode();
        return Math.abs(result) % seed;
    }

    public Tuple3<Long, Long, Long> sortTuple3(long a, long b, long c) {

        long[] longArray = new long[]{a, b, c};
        Arrays.sort(longArray);
        //System.out.println(longArray);
        return new Tuple3<Long, Long, Long>(longArray[0], longArray[1], longArray[2]);
    }

    public ArrayList<Tuple2<String, String>> getTuple2List(String a, String b) {

        ArrayList<Tuple2<String, String>> temp = new ArrayList<Tuple2<String, String>>();
        temp.add(new Tuple2(a, b));
        return temp;
    }

    public Tuple2<String, String> makeTuple2(String node1, String node2) {

        if (node1.compareTo(node2) <= 0) {
            return new Tuple2<String, String>(node1, node2);
        } else {
            return new Tuple2<String, String>(node2, node1);
        }
    }

//    public static void main(String[] args) throws Exception {
//        Tuple3<Long, Long, Long> a = sortKeyToTuple3(1L,3L,2L);
//        System.out.println(a._1() + ", " + a._2() + ", " + a._3());
//    }
}
