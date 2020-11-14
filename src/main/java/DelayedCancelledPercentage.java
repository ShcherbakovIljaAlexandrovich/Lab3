import java.io.Serializable;

public class DelayedCancelledPercentage implements Serializable {
    private float total;
    private int count;
    private float maxDelay;

    public float getTotal() {
        return total;
    }

    public int getCount() {
        return count;
    }

    public float getMaxDelay() {return maxDelay; }

    public DelayedCancelledCalculator(float total, int count, float maxDelay) {
        this.count = count;
        this.total = total;
        this.maxDelay = maxDelay;
    }

    public static DelayedCancelledCalculator addValue
            (DelayedCancelledCalculator a, float value) {
        return new DelayedCancelledCalculator(a.getTotal() + value,a.getCount() + 1);
    }
    public static DelayedCancelledCalculator add
            ( DelayedCancelledCalculator a, DelayedCancelledCalculator b) {
        return new DelayedCancelledCalculator(
                a.getTotal() + b.getTotal(),
                a.getCount() +b.getCount()
        );
    }
    public float avg() {
        return total / count;
    }
}
