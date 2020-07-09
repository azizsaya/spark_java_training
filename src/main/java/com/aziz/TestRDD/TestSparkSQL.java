package com.aziz.TestRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import scala.Tuple2;

public class TestSparkSQL {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession
                .builder()
                .appName("spark session example")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("C:\\Users\\Admin\\Desktop\\RPS-I\\SparkTraining\\src\\main\\resources\\JPMC\\students.csv");

        dataset.filter("subject='German' and year='2005'").show();

        Dataset<Row> modernArtResults = dataset.filter(Row->Row.getAs("subject").equals("Modern Art") && Integer.parseInt(Row.getAs("year"))>2007);
        modernArtResults.show();

        long num_of_Rows=dataset.count();
        System.out.println("There are " + num_of_Rows + " rows");

        System.out.println(dataset.first());

        String subject = dataset.first().getAs("year");

        System.out.println(subject);

        Column subjectColumn= dataset.col("subject"); ///column import from org.apache.spark.sql

//NOw We can Use This Object in Expression

        Dataset<Row> filtered= dataset.filter(subjectColumn.equalTo("Modern Art"));

        filtered.show();

             dataset.createOrReplaceTempView("my_student");

        Dataset<Row> results = spark.sql("select max(score),count (*), avg(score), subject from my_student group by subject");
        results.show();
        spark.close();


    }
}