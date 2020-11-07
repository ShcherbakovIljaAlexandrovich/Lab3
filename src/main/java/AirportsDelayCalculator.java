import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsDelayCalculator {
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DEST_AIRPORT_ID_COLUMN = 14;
    public static final int ARR_DELAY_NEW_COLUMN = 18;
    public static final String DELIMITER = ",";

    private static Tuple2<Tuple2<String, String>, Float> stringToDelayPair(String s) {
        String[] seq = s.split(DELIMITER);
        Tuple2<String, String> first = new Tuple2<>(seq[ORIGIN_AIRPORT_ID_COLUMN], seq[DEST_AIRPORT_ID_COLUMN]);
        float second = Float.parseFloat(seq[ARR_DELAY_NEW_COLUMN]);
        return new Tuple2<>(first, second);
    }

    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightInfo = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Float> delays =
                flightInfo.mapToPair(AirportsDelayCalculator::stringToDelayPair);

        JavaPairRDD<Tuple2<String, String>, AvgDelay> avgDelays =
                delays.combineByKey(
                p -> new AvgDelay(p, 1),
                (AvgDelay, p) -> AvgDelay.addValue(
                AvgDelay,
                p),
                AvgDelay::add);

        JavaRDD<String> airportsInfo = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportsLookup =
                airportsInfo.mapToPair(s -> {
                    String[] seq = s.split(DELIMITER);
                    return new Tuple2<>(seq[0], seq[1]);
                });

        
    }
}
