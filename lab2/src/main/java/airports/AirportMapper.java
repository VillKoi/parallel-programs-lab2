package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // code — идентификатор аэропорта
        // description — название аэропорта
        System.out.println("start AirportMapper");
        String text = value.toString();
        if (text.contains("Code")) {
            return;
        }

        String[] values = text.split("\",\"");
        String airportID = values[0].replaceAll("\"", "");
        String airportName = values[1].replaceAll("\"", "");

        context.write(new AirportWritableComparable(airportID, 0), new Text(airportName));
    }
}
