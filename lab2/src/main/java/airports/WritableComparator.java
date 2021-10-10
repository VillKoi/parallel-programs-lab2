package airports;

import org.apache.hadoop.io.WritableComparable;

public class WritableComparator {
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}
