package airports;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        (AirportWritableComparable) airport = (AirportWritableComparable) a;
        return airport.compareAirportID(b);
    }
}