import scala.Tuple2;

import java.io.Serializable;

public class DelayedCancelled implements Serializable {
    private int total;
    private int count;
    private float maxDelay;

    public int getTotal() {
        return total;
    }

    public int getCount() {
        return count;
    }

    public float getMaxDelay() {
        return maxDelay;
    }

    public DelayedCancelled(int total, int count, float maxDelay) {
        this.count = count;
        this.total = total;
        this.maxDelay = maxDelay;
    }

    public static DelayedCancelled addValue
            (DelayedCancelled a, Tuple2<Float, Float> value) {
        boolean delayedOrCancelled = value._1>0 || value._2>0;
        if (delayedOrCancelled) {
            return new DelayedCancelled(a.getTotal() + 1,a.getCount() + 1, Math.max(a.getMaxDelay(), value._1));
        }
        else {
            return new DelayedCancelled(a.getTotal(), a.getCount() + 1, Math.max(a.getMaxDelay(), value._1));
        }
    }
    public static DelayedCancelled add
            ( DelayedCancelled a, DelayedCancelled b) {
        return new DelayedCancelled(
                a.getTotal() + b.getTotal(),
                a.getCount() +b.getCount(),
                Math.max(a.getMaxDelay(), b.getMaxDelay())
        );
    }
    public float avg() {
        return total / (float)count;
    }
}
