package airports;


import org.apache.hadoop.mapreduce.Partitioner;

public class AirportPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
    @Override
    public int getPartition(KEY key, VALUE value, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}