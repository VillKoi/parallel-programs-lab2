package airports;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirportWritableComparable airport1 = (AirportWritableComparable) a;
        AirportWritableComparable airport2 = (AirportWritableComparable) b;
        return airport1.compareAirportID(airport2);
    }
}