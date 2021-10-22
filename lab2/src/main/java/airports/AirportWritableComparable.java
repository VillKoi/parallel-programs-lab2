package airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>{
    private String code;
    private Text value;

    public AirportWritableComparable(String code){
        this.code = code;
    }

    public void write(DataOutput var1) throws IOException{

    }

    public void readFields(DataInput var1) throws IOException{

    }

    public Text getFirst(){
        return this.value;
    }

    //    public int compare(WritableComparable a, WritableComparable b) {
//        return a.compareTo(b);
//    }

    @Override
    public int compareTo(AirportWritableComparable o) {
        return 0;
    }
}
