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

    
}
