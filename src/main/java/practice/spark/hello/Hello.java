package practice.spark.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zzp on 8/15/16.
 */
public class Hello {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> in = sc.textFile("README.md");
        System.out.println(in.count());
        System.out.println(in.first());
    }
}
