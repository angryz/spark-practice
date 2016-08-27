package practice.spark.hello;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Int;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author Zheng Zhipeng
 */
public class SortByKeyDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> pairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(2, "banana"),
                new Tuple2<>(1, "apple"),
                new Tuple2<>(4, "durian"),
                new Tuple2<>(3, "cherry"),
                new Tuple2<>(6, "grape"),
                new Tuple2<>(5, "fig"),
                new Tuple2<>(7, "haw")
        ));

        JavaPairRDD<Integer, String> result1 = pairs.sortByKey(false);
        List<Tuple2<Integer, String>> list1 = result1.collect();
        for (Tuple2<Integer, String> x : list1) {
            System.out.println(x._1() + " : " + x._2());
        }

        JavaPairRDD<Integer, String> result2 = pairs.sortByKey(new IntegerComparator(), false);
        List<Tuple2<Integer, String>> list2 = result2.collect();
        for (Tuple2<Integer, String> x : list2) {
            System.out.println(x._1() + " : " + x._2());
        }
    }

    // this must implements Serializable, or else exception will be throwed.
    static class IntegerComparator implements Comparator<Integer>, Serializable {

        @Override
        public int compare(Integer a, Integer b) {
            String c = String.valueOf(a);
            String d = String.valueOf(b);
            return c.compareTo(d);
        }
    }
}
