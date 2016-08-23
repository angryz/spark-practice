package practice.spark.hello;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Pair RDD 常用操作
 *
 * @author Zheng Zhipeng
 */
public class PairRddOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // mapValues(), reduceByKey()
        // 计算各个 key 的平均值
        JavaPairRDD<String, Integer> numbers = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 2),
                new Tuple2<>("cherry", 3),
                new Tuple2<>("durian", 4),
                new Tuple2<>("apple", 5),
                new Tuple2<>("cherry", 6),
                new Tuple2<>("cherry", 7)

        ));
        JavaPairRDD<String, AvgCount> maped = numbers.mapValues(v1 -> new AvgCount(v1, 1));
        JavaPairRDD<String, AvgCount> reduced = maped.reduceByKey(
                (v1, v2) -> new AvgCount(v1.total + v2.total, v1.num + v2.num)
        );
        System.out.println(StringUtils.join(reduced.collect(), ", "));
        JavaPairRDD<String, Double> avg = reduced.mapValues(v1 -> v1.avg());
        System.out.println("Avg : " + StringUtils.join(avg.collect(), ", "));

        // 计算单词数
        JavaRDD<String> input = sc.textFile("src/main/resources/spark-readme.md");
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split("\\s")));
        JavaPairRDD<String, Integer> result = words
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("Words count: " + result.takeSample(true, 20));
        // faster way to do the same
        //input.flatMap(x -> Arrays.asList(x.split("\\s"))).countByValue();
    }

    private static class AvgCount implements Serializable {

        public int total;
        public int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total / (double) num;
        }

        @Override
        public String toString() {
            return "AvgCount{" +
                    "total=" + total +
                    ", num=" + num +
                    '}';
        }
    }
}
