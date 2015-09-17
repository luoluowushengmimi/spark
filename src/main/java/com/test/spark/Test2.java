package com.test.spark;

import com.test.graphx.AllPathes$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by lenovo on 2015/9/7.
 */
public class Test2 {
    public static void main(String[] args) {
        String originalId = "<http://dbpedia.org/resource/Pay_It_Forward>";
        String destId = "<http://data.linkedmdb.org/resource/film/1112>";
        SparkContext sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local"));
        String[] pathes = AllPathes$.MODULE$.getAllPathes(originalId, destId, sc);
        //System.out.print(pathes);
        for(String s : pathes) {
            System.out.println(s);
        }
    }
}
