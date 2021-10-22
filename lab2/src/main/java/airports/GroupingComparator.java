package airports;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;

public class GroupingComparator implements RawComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}