package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // DEST_AEROPORT_ID имеет номер 14
        String text = value.toString();

        if (text.contains("YEAR")) {
            return;
        }

//        ARR_DELAY_NEW -  разница в минутах между расчетным временем приземления и реальным (>=0)
        String[] values = text.split(",");
        String destAirportID = values[14].replaceAll("\"", "");
        String delayingTime = values[18].replaceAll("\"", "");

        if (delayingTime.equals("0.00")  || delayingTime.length() == 0)  {
            return;
        }

        float delay = Float.parseFloat(delayingTime);

        context.write(new AirportWritableComparable(destAirportID, 1), new Text(String.valueOf(delay)));
    }
}