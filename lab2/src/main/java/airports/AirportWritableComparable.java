package airports;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparator {
    public void write(DataOutput var1) throws IOException{

    }

    public void readFields(DataInput var1) throws IOException{

    }

    public int compareTo(T o) {

    }

    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}
