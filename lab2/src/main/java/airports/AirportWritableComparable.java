package airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>{
    private Text value;
    private String airportID;

    private int indicator;

    public AirportWritableComparable() {
        this.value = new  Text("");
        this.airportID = "";
    }

    public AirportWritableComparable(String code, int indicator){
        this.airportID = code;
        this.indicator = indicator;
    }

    public void write(DataOutput var1) throws IOException{
        var1.writeChars(this.airportID);
    }

    public void readFields(DataInput var1) throws IOException{
        this.airportID = var1.toString();
    }

    public Text getFirst(){
        return this.value;
    }

    @Override
    public int compareTo(AirportWritableComparable o) {
        return 0;
    }

    public int compareAirportID(AirportWritableComparable item) {
        return (this.airportID.equalsIgnoreCase(item.airportID)) ? 0 : 1;
    }
}
