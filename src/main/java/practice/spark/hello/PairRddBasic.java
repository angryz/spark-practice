package practice.spark.hello;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 键值对类型 RDD: pair RDD 基本操作
 *
 * @author Zheng Zhipeng
 */
public class PairRddBasic {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 使用每一行的第一个单词创建一个 pair RDD
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "foo bar", "good job"));
        PairFunction<String, String, String> keyData =
                (PairFunction<String, String, String>) s -> new Tuple2<>(s.split(" ")[0], s);
        JavaPairRDD<String, String> pairs = lines.mapToPair(keyData);
        System.out.println(StringUtils.join(pairs.collect(), ", "));

        // 筛选掉长度超过10个字符的行
        JavaPairRDD<String, String> result = pairs.filter(kv -> (kv._2().length() < 10));
        System.out.println(StringUtils.join(result.collect(), ", "));
    }
}
