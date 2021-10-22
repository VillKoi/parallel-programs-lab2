package airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>{
    private int airportID;
    private int indicator;

    public AirportWritableComparable() {
        this.airportID = 0;
    }

    public AirportWritableComparable(String code, int indicator){
        this.airportID = Integer.parseInt(code);
        this.indicator = indicator;
    }

    @Override
    public void write(DataOutput var1) throws IOException{
        var1.writeInt(this.airportID);
        var1.writeInt(this.indicator);
    }
    @Override
    public void readFields(DataInput var1) throws IOException{
        this.airportID = var1.readInt();
        this.indicator = var1.readInt();
    }

    public int getAirportID(){
        return this.airportID;
    }

    @Override
    public int compareTo(AirportWritableComparable o) {
        return 0;
    }

    public int compareAirportID(AirportWritableComparable item) {
        return (this.airportID != item.airportID) ? 0 : 1;
    }
}
