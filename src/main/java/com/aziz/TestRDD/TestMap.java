package com.aziz.TestRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.Sqrt;
import java.util.ArrayList;
import java.util.List;

import java.util.ArrayList;

import static java.lang.Math.sqrt;

public class TestMap {
    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data = new ArrayList<>();
        input_data.add(20);
        input_data.add(21);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> myRDD = sc.parallelize(input_data);
        JavaRDD<Double> sqrRDD = myRDD.map(value -> sqrt(value));
        sqrRDD.collect().forEach(System.out::println);


        }
    }
