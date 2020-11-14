import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Comparator;

class DelayComparator implements Comparator<Tuple2<Tuple2<String, String>, Tuple2<Float, Float>>> {
    public int compare(Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> x,
                       Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> y) {
        return Float.compare(x._2._1, y._2._1);
    }
}

public class AirportsDelayCalculator {
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DEST_AIRPORT_ID_COLUMN = 14;
    public static final int ARR_DELAY_NEW_COLUMN = 18;
    public static final int CANCELLED_COLUMN = 18;
    public static final String DELIMITER = ",";

    private static Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> stringToDelayCancelPair(String s) {
        String[] seq = s.split(DELIMITER);
        Tuple2<String, String> first = new Tuple2<>(seq[ORIGIN_AIRPORT_ID_COLUMN], seq[DEST_AIRPORT_ID_COLUMN]);
        Tuple2<Float, Float> second = new Tuple2<>(Float.parseFloat(seq[ARR_DELAY_NEW_COLUMN]),
                                    Float.parseFloat(seq[CANCELLED_COLUMN]));
        return new Tuple2<>(first, second);
    }

    

    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightInfo = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Tuple2<Float, Float>> delayedCancelled =
                flightInfo.mapToPair(AirportsDelayCalculator::stringToDelayCancelPair);

        JavaPairRDD<Tuple2<String, String>, AvgDelay> avgDelays =
                delayedCancelled.combineByKey(
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
