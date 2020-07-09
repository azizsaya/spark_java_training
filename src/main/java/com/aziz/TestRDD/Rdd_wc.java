package com.aziz.TestRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

public class Rdd_wc {
    public static void main(String args[]) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext spContext = new JavaSparkContext(conf);

        JavaRDD<String> autoData = spContext.textFile("C:\\Users\\Admin\\Desktop\\RPS-I\\SparkTraining\\src\\main\\resources\\JPMC\\auto-data.csv");
        LongAccumulator sedanCount = spContext.sc().longAccumulator();
        LongAccumulator hatchbackCount = spContext.sc().longAccumulator();

        Broadcast<String> sedanText = spContext.broadcast("sedan");
        Broadcast<String> hatchbackText = spContext.broadcast("hatchback");

        JavaRDD<String> autoOut
                = autoData.map(new Function<String, String>() {
            public String call(String x) {

                if (x.contains(sedanText.value())) {
                    sedanCount.add(1);
                }
                if (x.contains(hatchbackText.value())) {
                    hatchbackCount.add(1);
                }
                return x;
            }
        });

        // Execute an action to force the map. Otherwise accumulators are not
        // triggered.
        autoOut.count();

        System.out.println("Demo for Accumulators and Broadcasts : ");
        System.out.println("Sedan Count : " + sedanCount.value() +
                "  HatchBack Count : " + hatchbackCount.value());
        spContext.close();
    }
}
