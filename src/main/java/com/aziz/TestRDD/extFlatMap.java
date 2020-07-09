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

public class extFlatMap {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\aziz.txt");
        JavaRDD<String> words = autoAllData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();}
        });
        System.out.println("Word Count is ::::   "+words.count());
        words.saveAsTextFile("C:\\Users\\Admin\\Desktop\\RPS-I\\aziz3.txt");
        words.collect().forEach(System.out::println);
        words.collect().forEach(System.out::println);
        sc.close();


    }
}
