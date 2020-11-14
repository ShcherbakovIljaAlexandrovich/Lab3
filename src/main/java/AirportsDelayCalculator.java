import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;

class DelayComparator implements Comparator<Tuple2<Tuple2<String, String>, Tuple2<Float, Float>>> {
    @Override
    public int compare(Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> o1, Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> o2) {
        return Float.compare(o1._2._1, o2._2._1);
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

        float maxDelay = delayedCancelled.max((o1, o2) -> Float.compare(o1._2._1, o2._2._1))._2._1;

        JavaPairRDD<Tuple2<String, String>, DelayedCancelledPercentage> delayedCancelledPercentage =
                delayedCancelled.combineByKey(
                        p -> {
                            boolean delayedOrCancelled = p._1>0 || p._2>0;
                            if (delayedOrCancelled) {
                                return new DelayedCancelledPercentage(1,1);
                            }
                            else {
                                return new DelayedCancelledPercentage(0,1);
                            }
                        },
                        DelayedCancelledPercentage::addValue,
                        DelayedCancelledPercentage::add);

        JavaRDD<String> airportsInfo = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportsLookup =
                airportsInfo.mapToPair(s -> {
                    String[] seq = s.split(DELIMITER);
                    return new Tuple2<>(seq[0], seq[1]);
                });
        Map<String, String> 
    }
}
