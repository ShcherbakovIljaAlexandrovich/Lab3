import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsDelayCalculator {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile(Hadoop "war-and");
    }
}
