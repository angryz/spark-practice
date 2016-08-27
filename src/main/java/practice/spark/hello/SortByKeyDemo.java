package practice.spark.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Zheng Zhipeng
 */
public class SortByKeyDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> pairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "apple"),
                new Tuple2<>(2, "banana"),
                new Tuple2<>(3, "cherry"),
                new Tuple2<>(4, "durian"),
                new Tuple2<>(5, "fig"),
                new Tuple2<>(6, "grape"),
                new Tuple2<>(7, "haw")
        ));

        JavaPairRDD result = pairs.sortByKey(
                (a, b) -> Integer.toString(a).compareTo(Integer.toString(b)),
                false
        );
        Map<Integer, String> resultMap = result.collectAsMap();
        for (Map.Entry<Integer, String> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
