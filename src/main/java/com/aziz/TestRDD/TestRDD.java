package com.aziz.TestRDD;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestRDD {

    public static void main(String args[]) throws InterruptedException {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\aziz.txt");

        //SqrtRdd.foreach(value->System.out.println(value));
        //New Way of Foreach in Java 1.8 is
        //SqrtRdd.foreach(System.out::println);

        autoAllData.collect().forEach(System.out::println);
        System.out.print(autoAllData);

        //How to Count How many Elements are There in RDD
        // how many elements in sqrtRdd
        // using just map and reduce
        //	JavaRDD<Long> singleIntegerRdd = SqrtRdd.map( value -> 1L);
        //Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        //System.out.println(count);

        sc.close();


    }

}

