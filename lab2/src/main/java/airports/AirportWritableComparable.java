package airports;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>{
    private int airportID;
    private int indicator;

    public AirportWritableComparable() {
        this.airportID = 0;
        this.indicator = 0;
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
    public int compareTo(AirportWritableComparable airport) {
        int airC = this.compareAirportID(airport);

        if (airC != 0) {
            return airC;
        }

        if (this.indicator == airport.indicator) {
            return 0;
        }

        return (this.indicator > airport.indicator) ? 1 : -1;
    }

    public int compareAirportID(AirportWritableComparable airport) {
        if (this.airportID == airport.airportID) {
            return 0;
        }

        return (this.airportID > airport.airportID) ? 1 : -1;
    }
}
