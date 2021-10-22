package airports;


public abstract class Partitioner<KEY, VALUE> extends Partitioner<> {
    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {

    }
}