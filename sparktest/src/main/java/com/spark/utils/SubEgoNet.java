package com.spark.utils;

import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by stefanie on 12/21/16.
 */
public class SubEgoNet {

    private ArrayList<Tuple2<String, String>> edgeList = new ArrayList<Tuple2<String, String>>();
    private ArrayList<String> pointList = new ArrayList<String>();

    public SubEgoNet() {

    }

    public void addNewEdge(Tuple2<String, String> t) {

        String node1 = t._1();
        String node2 = t._2();

        if (!pointList.contains(node1)) {
            pointList.add(node1);
        } else if(!pointList.contains(node2)) {
            pointList.add(node2);
        }

        if (!edgeList.contains(t)) {
            edgeList.add(t);
        }

        return;
    }

    public ArrayList<Tuple2<String, String>> getRecommandEdges() {

        ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
        EgoUtils eu = new EgoUtils();

        for (int i = 0; i < pointList.size() - 1; i++) {
            String node1 = pointList.get(i);
            for (int j = i + 1; j < pointList.size(); j++) {
                String node2 = pointList.get(j);
                Tuple2<String, String> newEdge = eu.makeTuple2(node1, node2);
                if (!edgeList.contains(newEdge)) {
                    result.add(newEdge);
                }
            }
        }

        return result;
    }

    public ArrayList<Tuple2<String, String>> getEdgeList() {
        return edgeList;
    }

    public ArrayList<String> getPointList() {return pointList;}
}
