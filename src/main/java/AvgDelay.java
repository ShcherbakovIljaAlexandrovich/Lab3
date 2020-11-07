import java.io.Serializable;

public class AvgDelay implements Serializable {
    private float total;
    private int count;

    public float getTotal() {
        return total;
    }

    public int getCount() {
        return count;
    }

    public AvgDelay(float total, int count) {
        this.count = count;
        this.total = total;
    }

    public static AvgDelay addValue
            (AvgDelay a, float value) {
        return new AvgDelay(
                a.getTotal() + value,
                a.getCount() + 1);
    }
    public static AvgDelay add
            ( AvgDelay a, AvgDelay b) {
        return new AvgDelay(
                a.getTotal() + b.getTotal(),
                a.getCount() +b.getCount()
        );
    }
    public float avg() {
        return total / count;
    }
}
