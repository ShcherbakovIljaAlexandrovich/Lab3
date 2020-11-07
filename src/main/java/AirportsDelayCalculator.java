import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class AirportsDelayCalculator {
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DEST_AIRPORT_ID_COLUMN = 14;
    public static final int ARR_DELAY_NEW_COLUMN = 18;
    public static final String delimiter = ",";
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<Tuple2<Tuple2<String, String>, Float>> delays = distFile.flatMap(
                s -> {

                }
        );
    }
}
