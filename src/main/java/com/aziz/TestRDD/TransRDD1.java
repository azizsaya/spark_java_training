package com.aziz.TestRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;
import java.util.ArrayList;
import static java.lang.Math.sqrt;

public class TransRDD1 {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\SparkTraining\\src\\main\\resources\\JPMC\\auto-data.csv");
        System.out.println("Count of original " + autoAllData.count());

        //chnage to TSV
        JavaRDD<String> tsvRDD = autoAllData.map(str -> str.replace(",","\t"));

        // Remove header
        String header = autoAllData.first();
        JavaRDD<String> autoData = autoAllData.filter(s -> !s.equals(header));

        autoAllData.take(5).forEach(System.out::println);


    }
}