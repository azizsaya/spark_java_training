package com.aziz.TestRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestRDD1 {

    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\aziz1.txt");
        autoAllData.collect().forEach(System.out::println);
        System.out.print(autoAllData);
        sc.close();
    }

}

