package com.spark.egonet;

import com.spark.utils.EgoUtils;
import com.spark.utils.SubEgoNet;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by stefanie on 12/21/16.
 */

public class RecommandAlgorithm {

    EgoUtils eu = new EgoUtils();

    String egoNode = null;
    ArrayList<Tuple2<String, String>> edgeList = new ArrayList<Tuple2<String, String>>();
    ArrayList<SubEgoNet> subEgoNetList = new ArrayList<SubEgoNet>();

    public RecommandAlgorithm(String egoNode, ArrayList<Tuple2<String, String>> edgeList) {

        this.egoNode = egoNode;
        this.edgeList = edgeList;
    }

    private void deleteEgoEdge() {
        ArrayList<Tuple2<String, String>> temp = new ArrayList<Tuple2<String, String>>();

        for (Tuple2<String, String> e : edgeList) {
            if (edgeContainsNode(e, egoNode)) {
                temp.add(e);
            }
        }

        for (Tuple2<String, String> e : temp) {
            edgeList.remove(e);
        }
        return;
    }

    private boolean edgeContainsNode (Tuple2<String, String> edge, String node) {

        if (edge._1().equals(node) || edge._2().equals(node)) {
            return true;
        } else {
            return false;
        }
    }

    private ArrayList<String> findNeighbourNodes(String node) {

        ArrayList<String> result = new ArrayList<String>();

        for (Tuple2<String, String> e : edgeList) {
            if (e._1().equals(node)) {
                result.add(e._2());
            } else if (e._2().equals(node)) {
                result.add(e._1());
            }
        }
        return result;
    }

    public ArrayList<SubEgoNet> findSubEgoNet() {

        while(edgeList.size() != 0) {

            ArrayList<String> tempNodeList = new ArrayList<String>();
            SubEgoNet subNet = new SubEgoNet();

            Tuple2<String, String> startEdge = edgeList.get(0);

            tempNodeList.add(startEdge._1());
            tempNodeList.add(startEdge._2());

            subNet.addNewEdge(startEdge);

            for (int i = 0; i < tempNodeList.size(); i++) {

                String node = tempNodeList.get(i);
                ArrayList<String> neighbourNode = findNeighbourNodes(node);

                for (String n : neighbourNode) {
                    if (!tempNodeList.contains(n)) {
                        tempNodeList.add(n);
                    }
                    subNet.addNewEdge(eu.makeTuple2(node, n));
                    edgeList.remove(eu.makeTuple2(node, n));
                }
            }

            subEgoNetList.add(subNet);
        }
        return subEgoNetList;
    }

    public ArrayList<Tuple2<String, String>> runAlgorithm() {

        deleteEgoEdge();
        ArrayList<SubEgoNet> subNets = findSubEgoNet();
        ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();

        for (SubEgoNet subNet : subNets) {

            ArrayList<Tuple2<String, String>> recommandEdges = subNet.getRecommandEdges();
            result.addAll(recommandEdges);
        }

        return result;
    }

//    public ArrayList<Tuple2<String, String>> findRecommand() {
//
//        ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
//        return result;
//
//    }

}
