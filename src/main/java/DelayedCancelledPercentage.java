import scala.Tuple2;

import java.io.Serializable;

public class DelayedCancelledPercentage implements Serializable {
    private int total;
    private int count;

    public int getTotal() {
        return total;
    }

    public int getCount() {
        return count;
    }

    public DelayedCancelledPercentage(int total, int count) {
        this.count = count;
        boolean delayedOrCancelled = total._1>0 || total._2>0;
        if (delayedOrCancelled) {
            this.total = 1;
        }
        else {
            this.total = 0;
        }
    }

    public static DelayedCancelledPercentage addValue
            (DelayedCancelledPercentage a, Tuple2<Float, Float> value) {
        boolean delayedOrCancelled = value._1>0 || value._2>0;
        if (delayedOrCancelled) {
            return new DelayedCancelledPercentage(a.getTotal() + 1,a.getCount() + 1);
        }
        else {
            return new DelayedCancelledPercentage(a.getTotal(), a.getCount() + 1);
        }
    }
    public static DelayedCancelledPercentage add
            ( DelayedCancelledPercentage a, DelayedCancelledPercentage b) {
        return new DelayedCancelledPercentage(
                a.getTotal() + b.getTotal(),
                a.getCount() +b.getCount()
        );
    }
    public float avg() {
        return total / (float)count;
    }
}
