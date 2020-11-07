import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class AirportsDelayCalculator {
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DEST_AIRPORT_ID_COLUMN = 11;
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<Tuple2<String, String>, > delays = distFile.flatMap(
                s -> Arrays.stream(s.split(",")).iterator()
        );
    }
}
