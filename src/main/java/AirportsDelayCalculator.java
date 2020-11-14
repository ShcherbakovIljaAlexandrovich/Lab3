import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportsDelayCalculator {
    public static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DEST_AIRPORT_ID_COLUMN = 14;
    public static final int ARR_DELAY_NEW_COLUMN = 18;
    public static final int CANCELLED_COLUMN = 19;
    public static final String DELIMITER = ",";

    private static Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> stringToDelayCancelPair(String s) {
        String[] seq = s.split(DELIMITER);
        Tuple2<String, String> first = new Tuple2<>(
                String.format("\"%s\"",seq[ORIGIN_AIRPORT_ID_COLUMN]),
                String.format("\"%s\"",seq[DEST_AIRPORT_ID_COLUMN])
        );
        String delay = seq[ARR_DELAY_NEW_COLUMN];
        if (delay.equals("")) {delay = "0.00";}
        String cancelled = seq[CANCELLED_COLUMN];
        Tuple2<Float, Float> second = new Tuple2<>(Float.parseFloat(delay), Float.parseFloat(cancelled));
        return new Tuple2<>(first, second);
    }

    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightInfo = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Tuple2<Float, Float>> delayedCancelledInfo =
                flightInfo.mapToPair(AirportsDelayCalculator::stringToDelayCancelPair);

        JavaPairRDD<Tuple2<String, String>, DelayedCancelled> delayedCancelled =
                delayedCancelledInfo.combineByKey(
                        p -> {
                            boolean delayedOrCancelled = p._1>0 || p._2>0;
                            if (delayedOrCancelled) {
                                return new DelayedCancelled(1,1, p._1);
                            }
                            else {
                                return new DelayedCancelled(0,1, p._1);
                            }
                        },
                        DelayedCancelled::addValue,
                        DelayedCancelled::add);

        JavaRDD<String> airportsInfo = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportsLookup =
                airportsInfo.mapToPair(s -> {
                    String[] seq = s.split(DELIMITER);
                    return new Tuple2<>(seq[0], seq[1]);
                });
        Map<String, String> stringAirportDataMap = airportsLookup.collectAsMap();
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(stringAirportDataMap);
        JavaRDD<String> output = delayedCancelled.map(element -> {
            return String.format("%s to %s: max delay %f, delayed/cancelled percentage %f",
                    airportsBroadcasted.value().get(element._1._1),
                    airportsBroadcasted.value().get(element._1._2),
                    element._2.getMaxDelay(),
                    element._2.avg()
                    );
        });
        output.saveAsTextFile("output");
    }
}
