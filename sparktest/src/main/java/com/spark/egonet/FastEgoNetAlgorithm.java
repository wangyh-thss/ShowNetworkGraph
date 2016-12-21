package com.spark.egonet;

import com.spark.utils.EgoUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by stefanie on 12/21/16.
 */

public class FastEgoNetAlgorithm {

    /*
    make sure the edge in edges set are in order(node1 <= node2)
     */

    private EgoUtils eu = new EgoUtils();
    private ArrayList<Tuple2<String, String>> allEdges = new ArrayList<Tuple2<String, String>>();
    private ArrayList<Tuple2<String, String>> edgeSet = new ArrayList<Tuple2<String, String>>();
    private Map<String, ArrayList<String>> neighborMap = new HashMap<String, ArrayList<String>>();
    private Map<String, ArrayList<Tuple2<String, String>>> result =
            new HashMap<String, ArrayList<Tuple2<String, String>>>();

    public FastEgoNetAlgorithm(ArrayList<Tuple2<String, String>> allEdges, ArrayList<Tuple2<String, String>> edges) {
        edgeSet = edges;
        this.allEdges = allEdges;
    }

    private String findMinDegreeNode() {

        neighborMap.clear();

        String egoNode = "";
        int egoMin = Integer.MAX_VALUE;


        for (Tuple2<String, String> e : edgeSet) {

            String node1 = e._1();
            String node2 = e._2();

            if (neighborMap.containsKey(node1)) {
                ArrayList<String> nodeList = neighborMap.get(node1);
                if (!nodeList.contains(node2)) {
                    nodeList.add(node2);
                    neighborMap.put(node1, nodeList);
                }

            } else {
                ArrayList<String> nodeList = new ArrayList<String>();
                nodeList.add(node2);
                neighborMap.put(node1, nodeList);
            }

            if (neighborMap.containsKey(node2)) {
                ArrayList<String> nodeList = neighborMap.get(node2);
                if (!nodeList.contains(node1)) {
                    nodeList.add(node1);
                    neighborMap.put(node2, nodeList);
                }
            } else {
                ArrayList<String> nodeList = new ArrayList<String>();
                nodeList.add(node1);
                neighborMap.put(node2, nodeList);
            }
        }

        Iterator<String> iter = neighborMap.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            if (neighborMap.get(key).size() < egoMin) {
                egoNode = key;
                egoMin = neighborMap.get(key).size();
            } else {
                continue;
            }
        }

        return egoNode;
    }

    private boolean checkInEdges(String node1, String node2) {

        Tuple2<String, String> t1 = eu.makeTuple2(node1, node2);

        if (allEdges.contains(t1)) {
            return true;
        } else {
            return false;
        }
    }

    private void addEdgeToEgo(String node, Tuple2<String, String> edge) {

        ArrayList<Tuple2<String, String>> nodeList = new ArrayList<Tuple2<String, String>>();

        if (result.containsKey(node)) {
            nodeList = result.get(node);
            nodeList.add(edge);
            result.put(node, nodeList);
        } else {
            nodeList.add(edge);
            result.put(node, nodeList);
        }
    }

    private void deleteOneEdge(String node1, String node2) {

        Tuple2<String, String> t1 = eu.makeTuple2(node1, node2);

        if (edgeSet.contains(t1)) {
            edgeSet.remove(t1);
        }
    }

    private void deleteNodeEdges(String node) {

        ArrayList<String> neighbour = neighborMap.get(node);
        for (String n : neighbour) {
            deleteOneEdge(n, node);
        }
    }

    public Map<String, ArrayList<Tuple2<String, String>>> runAlgorithm() {
        
        while (edgeSet.size() != 0) {

            String u = findMinDegreeNode();
            ArrayList<String> neighbour = neighborMap.get(u);

            for (int i = 0; i < neighbour.size() - 1; i++) {

                String v = neighbour.get(i);
                addEdgeToEgo(u, eu.makeTuple2(u, v));
                addEdgeToEgo(v, eu.makeTuple2(v, u));

                for (int j = i + 1; j < neighbour.size(); j++) {
                    String z = neighbour.get(j);
                    if (checkInEdges(v, z)) {
                        addEdgeToEgo(z, eu.makeTuple2(u, v));
                        addEdgeToEgo(u, eu.makeTuple2(v, z));
                        addEdgeToEgo(v, eu.makeTuple2(u, z));
                    }
                }
            }
            String temp = neighbour.get(neighbour.size()-1);
            addEdgeToEgo(u, eu.makeTuple2(u, temp));
            addEdgeToEgo(temp, eu.makeTuple2(temp, u));
            deleteNodeEdges(u);
        }

        return result;
    }

}
