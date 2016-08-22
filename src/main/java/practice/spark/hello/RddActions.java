package practice.spark.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * RDD上常用的行动操作。
 *
 * @author Zheng Zhipeng
 */
public class RddActions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> oneToFour = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        // reduce()
        Integer sum = oneToFour.reduce((x, y) -> x + y);
        System.out.println("reduce() | sum = " + sum);

        // fold()
        Integer sum1 = oneToFour.fold(0, (x, y) -> x + y);
        System.out.println("fold() | sum = " + sum1);

        // map() reduce()
        JavaPairRDD<Integer, Integer> pair = oneToFour.mapToPair(x -> new Tuple2<>(x, 1));
        Tuple2<Integer, Integer> pairSum = pair.reduce(
                (p, q) -> new Tuple2<>(p._1() + q._1(), p._2() + q._2()));
        System.out.println("map(),reduce() | average = " + pairSum._1() / (double) pairSum._2());

        // aggregate()
        AvgCount init = new AvgCount(0, 0);
        AvgCount result = oneToFour.aggregate(init, addAndCount, combine);
        System.out.println("aggregate() | average = " + result.avg());
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
    }

    static Function2<AvgCount, Integer, AvgCount> addAndCount =
            (p, q) -> {
                p.total += q;
                p.num += 1;
                return p;
            };

    static Function2<AvgCount, AvgCount, AvgCount> combine =
            (p, q) -> {
                p.total += q.total;
                p.num += q.num;
                return p;
            };

}
