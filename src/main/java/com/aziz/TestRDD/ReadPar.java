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

public class ReadPar {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession
                .builder()
                .appName("spark session example")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> babyName = spark.read().parquet("C:\\Users\\Admin\\Desktop\\RPS-I\\SparkTraining\\src\\main\\resources\\JPMC\\baby_names.parquet");
        babyName.show();
        babyName.printSchema();

        //Do data frame queries
        System.out.println("SELECT Demo :");
        babyName.select(col("name"),col("prob")).show();
//
//        System.out.println("FILTER for Age == 40 :");
//        empDf.filter(col("age").equalTo(40)).show();
//
//        System.out.println("GROUP BY gender and count :");
//        empDf.groupBy(col("gender")).count().show();
//
//        System.out.println("GROUP BY deptId and find average of salary and max of age :");
//        Dataset<Row> summaryData = empDf.groupBy(col("deptid"))
//                .agg(avg(empDf.col("salary")), max(empDf.col("age")));
//        summaryData.show();
//
//
//        empDf.createOrReplaceTempView("employee");
//
//        Dataset<Row> summaryData_sql = spark.sql("select deptid, avg(salary), max(age) from employee group by deptid  ");
//        summaryData_sql.show();
//
//        spark.close();


    }
}