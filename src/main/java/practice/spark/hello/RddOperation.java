package practice.spark.hello;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author Zheng Zhipeng
 */
public class RddOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // map()
        JavaRDD<Integer> oneToFour = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = oneToFour.map(x -> x * x);
        System.out.println("map() | " + StringUtils.join(result.collect(), ","));

        // flatMap()
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s")));
        System.out.println("flatMap() | " + words.first());

        // filter()
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> oddNumbers = numbers.filter(n -> n % 2 != 0);
        System.out.println("filter() | " + StringUtils.join(oddNumbers.collect(), ", "));

        // distinct()
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList("apple", "pear", "banana", "apple"));
        JavaRDD<String> distincted = fruits.distinct();
        System.out.println("distinct() | " + fruits.count() + " -> " + distincted.count() + " | "
                + StringUtils.join(distincted.collect(), ", "));

        // sample()
        JavaRDD<Integer> samples = numbers.sample(false, 0.5);
        System.out.println("sample() | " + StringUtils.join(oddNumbers.collect(), ", "));

        // union()
        JavaRDD<String> animals = sc.parallelize(Arrays.asList("monkey", "lion", "tiger", "snake"));
        JavaRDD<String> animalsAndFruits = animals.union(fruits);
        System.out.println("union() | " + StringUtils.join(animalsAndFruits.collect(), ", "));

        // intersection()
        JavaRDD<String> animals1 = sc.parallelize(Arrays.asList("elephone", "eagle", "monkey",
                "snake"));
        JavaRDD<String> sameAnimals = animals1.intersection(animals);
        System.out.println("intersection() | " + StringUtils.join(sameAnimals.collect(), ", "));

        // subtract()
        JavaRDD<String> fewAnimals = animals.subtract(animals1);
        System.out.println("subtract() | " + StringUtils.join(fewAnimals.collect(), ", "));

        // cartesian()
        JavaPairRDD<String, String> animalsWithFruits = animals.cartesian(distincted);
        System.out.println("cartesian() | " + StringUtils.join(animalsWithFruits.collect(), ", "));
    }
}
