package com.aziz.TestRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class countLine {

    public static void main(String args[]) throws InterruptedException {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\SparkTraining\\src\\main\\resources\\JPMC\\auto-data.csv");
        System.out.print(autoAllData.count());

        String header = autoAllData.first();
        JavaRDD<String> autoData = autoAllData.filter(s -> !s.equals(header));

        String shortest
                =autoData.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                    return (v1.length() < v2.length() ? v1 : v2);

            }
        });
        System.out.println(shortest);

        sc.close();


    }

}

