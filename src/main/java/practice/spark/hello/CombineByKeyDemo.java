package practice.spark.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * @author Zheng Zhipeng
 */
public class CombineByKeyDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> nums = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 2),
                new Tuple2<>("cherry", 3),
                new Tuple2<>("durian", 4),
                new Tuple2<>("apple", 5),
                new Tuple2<>("cherry", 6),
                new Tuple2<>("cherry", 7)
        ));

        JavaPairRDD result = nums.combineByKey(
                (Function<Integer, AvgCount>) x -> new AvgCount(x, 1),
                (Function2<AvgCount, Integer, AvgCount>) (a, x) -> {
                    a.total += x;
                    a.num += 1;
                    return a;
                },
                (Function2<AvgCount, AvgCount, AvgCount>) (a, b) -> {
                    a.total += b.total;
                    a.num += b.num;
                    return a;
                }
        );
        Map<String, AvgCount> resultMap = result.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue().avg());
        }
    }

    private static class AvgCount implements Serializable {

        public int total;
        public int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public float avg() {
            return total / (float) num;
        }
    }

}
