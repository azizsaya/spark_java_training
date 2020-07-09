package com.aziz.TestRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.Sqrt;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.ArrayList;

import static java.lang.Math.sqrt;

public class TestReduce {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> input_data = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> collData = sc.parallelize(input_data);
        collData.collect().forEach(System.out::println);

        int collCount = collData.reduce((x,y) -> x + y);
        System.out.println(collCount);
    }}