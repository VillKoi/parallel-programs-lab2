package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // DEST_AEROPORT_ID имеет номер 14

        String text  =   value.toString();

        if (text.contains("YEAR")) {
            return;
        }

        String[] values = text.split(",");
        String destAirportID = values[14];

        context.write(new AirportWritableComparable(destAirportID), new Text(text));
    }
}