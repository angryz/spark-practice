package practice.spark.hello;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * 持久化RDD数据到堆内存或磁盘。
 *
 * @author Zheng Zhipeng
 */
public class RddPersistence {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaRDD<Integer> square = input.map(x -> x * x);
        square.persist(StorageLevel.DISK_ONLY());

        System.out.println(square.count());
        System.out.println(StringUtils.join(square.collect(), ", "));

        square.unpersist();
    }
}
