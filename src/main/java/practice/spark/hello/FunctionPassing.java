package practice.spark.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

/**
 * @author Zheng Zhipeng
 */
public class FunctionPassing {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("README.md");

        // use anonymous inner class
        JavaRDD<String> errors = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("error");
            }
        });

        // use named inner class
        JavaRDD<String> errors1 = lines.filter(new ContainsError());

        // use named inner class with constructor method witch has arguments
        JavaRDD<String> errors2 = lines.filter(new Contains("error"));

        // use lambda
        JavaRDD<String> errors3 = lines.filter(s -> s.contains("error"));
    }

    public static class ContainsError implements Function<String, Boolean> {

        @Override
        public Boolean call(String v1) throws Exception {
            return v1.contains("error");
        }
    }

    public static class Contains implements Function<String, Boolean> {

        private String query;

        public Contains(String query) {
            this.query = query;
        }

        @Override
        public Boolean call(String v1) throws Exception {
            return v1.contains(query);
        }
    }
}
