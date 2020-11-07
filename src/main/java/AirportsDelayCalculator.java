import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class AirportsDelayCalculator {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> splitted = distFile.flatMap(
                s -> Arrays.stream(.split(" ")).iterator();
        );
    }
}
