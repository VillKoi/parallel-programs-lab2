package airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportPartitioner extends Partitioner<AirportWritableComparable, Text> {
    @Override
    public int getPartition(AirportWritableComparable key, Text value, int numReduceTasks) {
        return (key.getAirportID() & Integer.MAX_VALUE) % numReduceTasks;
    }
}